from __future__ import annotations

import html
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

DECISION_JOURNAL_SCHEMA_VERSION = "etf_portfolio_decision_journal_v1"
DECISION_JOURNAL_REPORT_SCHEMA_VERSION = "etf_portfolio_decision_journal_report_v1"
DECISION_JOURNAL_VALIDATION_SCHEMA_VERSION = "etf_portfolio_decision_journal_validation_v1"
DECISION_JOURNAL_ANALYTICS_SCHEMA_VERSION = "etf_portfolio_decision_journal_analytics_v1"
DECISION_STATE_PROPOSAL_SCHEMA_VERSION = "etf_portfolio_decision_state_proposal_v1"

DEFAULT_DECISION_JOURNAL_PATH = PROJECT_ROOT / "data" / "simulation" / (
    "etf_portfolio_decision_journal.json"
)
DEFAULT_DECISION_JOURNAL_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "decision_journal"
)
DEFAULT_DECISION_JOURNAL_VALIDATION_DIR = DEFAULT_DECISION_JOURNAL_REPORT_DIR / "validation"
DEFAULT_DECISION_JOURNAL_PROPOSAL_DIR = DEFAULT_DECISION_JOURNAL_REPORT_DIR / "proposals"

DECISION_JOURNAL_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

REQUIRED_DECISION_FIELDS: tuple[str, ...] = (
    "review_id",
    "decision_id",
    "review_date",
    "source_weekly_review",
    "action_item_id",
    "human_decision",
    "decision_status",
    "rationale",
    "confidence",
    "follow_up_task",
    "linked_candidate",
    "linked_report",
    "created_at",
)

DECISION_STATUS_VALUES = frozenset(
    {
        "accept_recommendation",
        "reject_recommendation",
        "defer_decision",
        "continue_observation",
        "mark_watch",
        "archive_candidate_after_review",
        "start_new_experiment",
        "request_more_data",
    }
)

DISALLOWED_DECISION_ACTIONS = frozenset(
    {
        "place_order",
        "enable_broker_action",
        "promote_to_production_without_governance",
    }
)

ACTION_LIKE_FIELDS = frozenset(
    {
        "human_decision",
        "decision_status",
        "follow_up_task",
        "candidate_state_proposal",
        "proposed_state_action",
    }
)


class DecisionJournalError(ValueError):
    """Raised when a decision journal entry violates the audited schema."""


def build_decision_entry(
    *,
    review_id: str,
    review_date: date | str,
    source_weekly_review: Path | str,
    action_item_id: str,
    human_decision: str,
    decision_status: str,
    rationale: str,
    confidence: float,
    follow_up_task: str,
    linked_candidate: str,
    linked_report: Path | str,
    created_at: datetime | None = None,
    decision_id: str | None = None,
    extra_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    created = created_at or datetime.now(tz=UTC)
    review_date_text = (
        review_date.isoformat() if isinstance(review_date, date) else str(review_date)
    )
    source_path = str(source_weekly_review)
    linked_report_text = str(linked_report)
    safe_decision_id = decision_id or _decision_id(
        review_id=review_id,
        action_item_id=action_item_id,
        decision_status=decision_status,
        created_at=created,
    )
    entry = {
        "review_id": _text(review_id),
        "decision_id": safe_decision_id,
        "review_date": review_date_text,
        "source_weekly_review": source_path,
        "action_item_id": _text(action_item_id),
        "human_decision": _text(human_decision),
        "decision_status": _text(decision_status),
        "rationale": _text(rationale),
        "confidence": float(confidence),
        "follow_up_task": _text(follow_up_task),
        "linked_candidate": _text(linked_candidate),
        "linked_report": linked_report_text,
        "created_at": created.isoformat(),
        "audit_trail": [
            {
                "event": "created",
                "timestamp": created.isoformat(),
                "source": "decision_journal_schema",
            }
        ],
        **DECISION_JOURNAL_SAFETY,
    }
    entry.update(dict(extra_fields or {}))
    validate_decision_entry(entry)
    return entry


def empty_decision_journal(generated_at: datetime | None = None) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    return {
        "schema_version": DECISION_JOURNAL_SCHEMA_VERSION,
        "journal_type": "etf_portfolio_decision_journal",
        "created_at": generated.isoformat(),
        "updated_at": generated.isoformat(),
        "entries": [],
        "removed_entries": [],
        **DECISION_JOURNAL_SAFETY,
    }


def load_decision_journal(path: Path = DEFAULT_DECISION_JOURNAL_PATH) -> dict[str, Any]:
    if not path.exists():
        return empty_decision_journal()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DecisionJournalError(f"decision journal is not valid JSON: {path}") from exc
    if not isinstance(payload, Mapping):
        raise DecisionJournalError("decision journal root must be a JSON object")
    journal = dict(payload)
    validate_decision_journal_schema(journal)
    return journal


def write_decision_journal(
    journal: Mapping[str, Any],
    path: Path = DEFAULT_DECISION_JOURNAL_PATH,
) -> Path:
    validate_decision_journal_schema(journal)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(journal, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def build_decision_entry_from_weekly_review(
    *,
    weekly_review_path: Path,
    action_item_id: str,
    human_decision: str,
    decision_status: str,
    rationale: str,
    confidence: float,
    follow_up_task: str,
    linked_candidate: str,
    linked_report: Path | None = None,
    created_at: datetime | None = None,
    decision_id: str | None = None,
) -> dict[str, Any]:
    weekly_review = load_weekly_review_report(weekly_review_path)
    action_item = find_weekly_review_action(weekly_review, action_item_id)
    review_date = _text(
        weekly_review.get("review_end_date"),
        _text(_mapping(weekly_review.get("requested_date_range")).get("end")),
    )
    report_path = linked_report or weekly_review_path
    source_evidence = _records(action_item.get("evidence"))
    entry = build_decision_entry(
        review_id=_text(weekly_review.get("review_id")),
        review_date=review_date,
        source_weekly_review=weekly_review_path,
        action_item_id=action_item_id,
        human_decision=human_decision,
        decision_status=decision_status,
        rationale=rationale,
        confidence=confidence,
        follow_up_task=follow_up_task,
        linked_candidate=linked_candidate,
        linked_report=report_path,
        created_at=created_at,
        decision_id=decision_id,
        extra_fields={
            "source_section": _source_section_for_action(action_item),
            "source_action_type": _text(action_item.get("action_type")),
            "source_action_status": _text(action_item.get("status"), "open"),
            "source_action_priority": _text(action_item.get("priority")),
            "source_recommended_reason": _text(action_item.get("recommended_reason")),
            "action_item_snapshot": dict(action_item),
            "source_evidence": source_evidence,
        },
    )
    validate_decision_entry_links(entry)
    return entry


def add_decision_entry(
    journal: Mapping[str, Any],
    entry: Mapping[str, Any],
    *,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    result = dict(journal)
    result.setdefault("schema_version", DECISION_JOURNAL_SCHEMA_VERSION)
    result.setdefault("journal_type", "etf_portfolio_decision_journal")
    result.setdefault("created_at", (updated_at or datetime.now(tz=UTC)).isoformat())
    result.setdefault("removed_entries", [])
    result.update(DECISION_JOURNAL_SAFETY)
    entries = _records(result.get("entries"))
    decision_id = _text(entry.get("decision_id"))
    if any(_text(item.get("decision_id")) == decision_id for item in entries):
        raise DecisionJournalError(f"duplicate decision_id: {decision_id}")
    new_entry = dict(entry)
    validate_decision_entry(new_entry)
    validate_decision_entry_links(new_entry)
    entries.append(new_entry)
    result["entries"] = entries
    result["updated_at"] = (updated_at or datetime.now(tz=UTC)).isoformat()
    validate_decision_journal_schema(result)
    return result


def update_decision_entry(
    journal: Mapping[str, Any],
    *,
    decision_id: str,
    updates: Mapping[str, Any],
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    result = dict(journal)
    entries = _records(result.get("entries"))
    timestamp = (updated_at or datetime.now(tz=UTC)).isoformat()
    changed = False
    allowed_update_fields = {
        "human_decision",
        "decision_status",
        "rationale",
        "confidence",
        "follow_up_task",
        "linked_candidate",
        "linked_report",
    }
    for entry in entries:
        if _text(entry.get("decision_id")) != decision_id:
            continue
        changed_fields: dict[str, Any] = {}
        for field, value in updates.items():
            if field not in allowed_update_fields or value is None:
                continue
            old_value = entry.get(field)
            entry[field] = float(value) if field == "confidence" else str(value)
            if old_value != entry[field]:
                changed_fields[field] = {"old": old_value, "new": entry[field]}
        if not changed_fields:
            raise DecisionJournalError("no journal fields changed")
        entry["updated_at"] = timestamp
        audit = _records(entry.get("audit_trail"))
        audit.append(
            {
                "event": "updated",
                "timestamp": timestamp,
                "changed_fields": changed_fields,
            }
        )
        entry["audit_trail"] = audit
        validate_decision_entry(entry)
        validate_decision_entry_links(entry)
        changed = True
        break
    if not changed:
        raise DecisionJournalError(f"decision_id not found: {decision_id}")
    result["entries"] = entries
    result["updated_at"] = timestamp
    validate_decision_journal_schema(result)
    return result


def remove_decision_entry(
    journal: Mapping[str, Any],
    *,
    decision_id: str,
    reason: str,
    removed_at: datetime | None = None,
) -> dict[str, Any]:
    if not _text(reason):
        raise DecisionJournalError("remove requires a reason")
    result = dict(journal)
    entries = _records(result.get("entries"))
    removed_entries = _records(result.get("removed_entries"))
    timestamp = (removed_at or datetime.now(tz=UTC)).isoformat()
    kept: list[dict[str, Any]] = []
    removed: dict[str, Any] | None = None
    for entry in entries:
        if _text(entry.get("decision_id")) == decision_id:
            removed = dict(entry)
            continue
        kept.append(entry)
    if removed is None:
        raise DecisionJournalError(f"decision_id not found: {decision_id}")
    audit = _records(removed.get("audit_trail"))
    audit.append({"event": "removed", "timestamp": timestamp, "reason": reason})
    removed["audit_trail"] = audit
    removed["removed_at"] = timestamp
    removed["removal_reason"] = reason
    removed_entries.append(removed)
    result["entries"] = kept
    result["removed_entries"] = removed_entries
    result["updated_at"] = timestamp
    validate_decision_journal_schema(result)
    return result


def decision_entries(journal: Mapping[str, Any]) -> list[dict[str, Any]]:
    validate_decision_journal_schema(journal)
    return _records(journal.get("entries"))


def build_decision_journal_analytics(
    journal: Mapping[str, Any],
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    entries = decision_entries(journal)
    status_counts: dict[str, int] = {}
    candidate_counts: dict[str, int] = {}
    confidence_values: list[float] = []
    follow_up_count = 0
    for entry in entries:
        status = _text(entry.get("decision_status"), "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        candidate = _text(entry.get("linked_candidate"), "UNKNOWN")
        candidate_counts[candidate] = candidate_counts.get(candidate, 0) + 1
        confidence = _float_or_none(entry.get("confidence"))
        if confidence is not None:
            confidence_values.append(confidence)
        if _text(entry.get("follow_up_task")):
            follow_up_count += 1
    average_confidence = (
        round(sum(confidence_values) / len(confidence_values), 6)
        if confidence_values
        else None
    )
    return {
        "schema_version": DECISION_JOURNAL_ANALYTICS_SCHEMA_VERSION,
        "report_type": "etf_portfolio_decision_journal_analytics",
        "generated_at": generated.isoformat(),
        "entry_count": len(entries),
        "decision_status_counts": status_counts,
        "candidate_decision_counts": candidate_counts,
        "average_confidence": average_confidence,
        "min_confidence": min(confidence_values) if confidence_values else None,
        "max_confidence": max(confidence_values) if confidence_values else None,
        "follow_up_task_count": follow_up_count,
        "entries_with_rationale_count": sum(
            1 for entry in entries if _text(entry.get("rationale"))
        ),
        **DECISION_JOURNAL_SAFETY,
    }


def build_candidate_state_update_proposals(
    journal: Mapping[str, Any],
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    entries = decision_entries(journal)
    proposals = []
    for entry in entries:
        status = _text(entry.get("decision_status"))
        proposals.append(
            {
                "proposal_id": "proposal-" + _text(entry.get("decision_id")),
                "decision_id": entry.get("decision_id"),
                "review_id": entry.get("review_id"),
                "action_item_id": entry.get("action_item_id"),
                "linked_candidate": entry.get("linked_candidate"),
                "decision_status": status,
                "proposed_state_action": _state_action_for_decision(status),
                "proposal_status": "manual_review_proposal_only",
                "mutation_allowed": False,
                "requires_governance_before_production": True,
                "rationale": entry.get("rationale"),
                "source_weekly_review": entry.get("source_weekly_review"),
                "linked_report": entry.get("linked_report"),
                **DECISION_JOURNAL_SAFETY,
            }
        )
    payload = {
        "schema_version": DECISION_STATE_PROPOSAL_SCHEMA_VERSION,
        "report_type": "etf_portfolio_decision_state_update_proposal",
        "generated_at": generated.isoformat(),
        "proposal_count": len(proposals),
        "proposals": proposals,
        "state_mutation_performed": False,
        "production_weights_mutated": False,
        "shadow_registry_mutated": False,
        **DECISION_JOURNAL_SAFETY,
    }
    if _contains_unsafe_key(payload):
        raise DecisionJournalError("decision state proposal contains disallowed output key")
    return payload


def build_decision_journal_report(
    journal: Mapping[str, Any],
    *,
    as_of: date,
    journal_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    entries = decision_entries(journal)
    analytics = build_decision_journal_analytics(journal, generated_at=generated)
    proposals = build_candidate_state_update_proposals(journal, generated_at=generated)
    return {
        "schema_version": DECISION_JOURNAL_REPORT_SCHEMA_VERSION,
        "report_type": "etf_portfolio_decision_journal_report",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "source_journal_path": "" if journal_path is None else str(journal_path),
        "review_metadata": {
            "entry_count": len(entries),
            "removed_entry_count": len(_records(journal.get("removed_entries"))),
            "journal_updated_at": journal.get("updated_at"),
            "schema_version": journal.get("schema_version"),
        },
        "human_decision_summary": analytics,
        "linked_candidates_and_reports": _linked_candidates_and_reports(entries),
        "confidence_rationale_notes": _confidence_rationale_notes(entries),
        "follow_up_tasks": _follow_up_tasks(entries),
        "audit_trail": _audit_trail(entries, _records(journal.get("removed_entries"))),
        "entries": entries,
        "candidate_state_update_proposals": proposals,
        **DECISION_JOURNAL_SAFETY,
    }


def build_decision_journal_validation_report(
    *,
    journal_path: Path = DEFAULT_DECISION_JOURNAL_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    checks: list[dict[str, Any]] = []
    journal: dict[str, Any] | None = None
    try:
        journal = load_decision_journal(journal_path)
        _append_check(checks, "journal_schema", True, "Decision journal schema is valid.")
    except DecisionJournalError as exc:
        _append_check(checks, "journal_schema", False, str(exc))
    if journal is not None:
        entries = _records(journal.get("entries"))
        link_failures = []
        for entry in entries:
            try:
                validate_decision_entry_links(entry)
            except DecisionJournalError as exc:
                link_failures.append({"decision_id": entry.get("decision_id"), "error": str(exc)})
        _append_check(
            checks,
            "entry_linkage",
            not link_failures,
            f"{len(entries) - len(link_failures)} of {len(entries)} entries have valid links.",
            {"failures": link_failures},
        )
        _append_check(
            checks,
            "manual_review_required",
            all(entry.get("manual_review_required") is True for entry in entries),
            "All active entries require manual review.",
        )
        _append_check(
            checks,
            "no_disallowed_output_keys",
            not _contains_unsafe_key(journal),
            "Journal does not contain disallowed production or broker output keys.",
        )
    _append_check(
        checks,
        "disallowed_actions_blocked",
        _disallowed_action_validation_passes(),
        "Schema rejects disallowed action fields.",
    )
    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    return {
        "schema_version": DECISION_JOURNAL_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_portfolio_decision_journal_validation",
        "status": status,
        "generated_at": generated.isoformat(),
        "journal_path": str(journal_path),
        "journal_path_exists": journal_path.exists(),
        "entry_count": 0 if journal is None else len(_records(journal.get("entries"))),
        "checks": checks,
        "production_weights_mutated": False,
        "broker_actions_created": False,
        **DECISION_JOURNAL_SAFETY,
    }


def render_decision_journal_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("review_metadata"))
    summary = _mapping(payload.get("human_decision_summary"))
    lines = [
        "# ETF Portfolio Decision Journal",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只记录人工复核结论，不修改 production weights、shadow registry 或 broker 状态。",
        "",
        "## Review Metadata",
        "",
        f"- As Of: {payload.get('as_of')}",
        f"- Entry Count: {metadata.get('entry_count')}",
        f"- Removed Entry Count: {metadata.get('removed_entry_count')}",
        f"- Journal Updated At: {metadata.get('journal_updated_at')}",
        "",
        "## Human Decision Summary",
        "",
        f"- Decision Status Counts: {summary.get('decision_status_counts')}",
        f"- Average Confidence: {summary.get('average_confidence')}",
        f"- Follow-up Task Count: {summary.get('follow_up_task_count')}",
        "",
        "## Linked Candidates and Reports",
        "",
        "| Candidate | Decision Count | Reports |",
        "|---|---:|---|",
    ]
    for item in _records(payload.get("linked_candidates_and_reports")):
        lines.append(
            f"| {item.get('linked_candidate')} | {item.get('decision_count')} | "
            f"{', '.join(_texts(item.get('linked_reports')))} |"
        )
    lines.extend(
        [
            "",
            "## Confidence / Rationale Notes",
            "",
            "| Decision | Status | Confidence | Rationale |",
        ]
    )
    lines.append("|---|---|---:|---|")
    for item in _records(payload.get("confidence_rationale_notes")):
        lines.append(
            f"| {item.get('decision_id')} | {item.get('decision_status')} | "
            f"{item.get('confidence')} | {item.get('rationale')} |"
        )
    lines.extend(["", "## Follow-up Tasks", "", "| Decision | Candidate | Follow-up |"])
    lines.append("|---|---|---|")
    for item in _records(payload.get("follow_up_tasks")):
        lines.append(
            f"| {item.get('decision_id')} | {item.get('linked_candidate')} | "
            f"{item.get('follow_up_task')} |"
        )
    lines.extend(["", "## Audit Trail", "", "| Decision | Event | Timestamp |"])
    lines.append("|---|---|---|")
    for item in _records(payload.get("audit_trail")):
        lines.append(
            f"| {item.get('decision_id')} | {item.get('event')} | {item.get('timestamp')} |"
        )
    return "\n".join(lines) + "\n"


def render_decision_journal_html(payload: Mapping[str, Any]) -> str:
    markdown = render_decision_journal_markdown(payload)
    escaped = html.escape(markdown)
    title = html.escape(f"ETF Portfolio Decision Journal {payload.get('as_of')}")
    return (
        "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">"
        f"<title>{title}</title>"
        "<style>body{font-family:Arial,sans-serif;max-width:1040px;margin:32px auto;"
        "line-height:1.5;color:#1f2937}pre{white-space:pre-wrap;background:#f8fafc;"
        "border:1px solid #e5e7eb;padding:16px}</style></head><body>"
        f"<pre>{escaped}</pre></body></html>\n"
    )


def render_decision_state_proposal_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Decision State Update Proposals",
        "",
        "- production_effect = none",
        "- broker_action = none",
        "- state_mutation_performed = false",
        "- shadow_registry_mutated = false",
        "",
        "| Decision | Candidate | Decision Status | Proposed State Action |",
        "|---|---|---|---|",
    ]
    for proposal in _records(payload.get("proposals")):
        lines.append(
            f"| {proposal.get('decision_id')} | {proposal.get('linked_candidate')} | "
            f"{proposal.get('decision_status')} | {proposal.get('proposed_state_action')} |"
        )
    return "\n".join(lines) + "\n"


def render_decision_journal_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Decision Journal Validation",
        "",
        f"- Status: {payload.get('status')}",
        f"- Journal Path: {payload.get('journal_path')}",
        f"- Entry Count: {payload.get('entry_count')}",
        "- production_effect = none",
        "- broker_action = none",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | {check.get('message')} |"
        )
    return "\n".join(lines) + "\n"


def write_decision_journal_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
    html_path: Path,
) -> None:
    _write_json(payload, json_path)
    _write_text(render_decision_journal_markdown(payload), markdown_path)
    _write_text(render_decision_journal_html(payload), html_path)


def write_decision_journal_analytics(payload: Mapping[str, Any], path: Path) -> None:
    _write_json(payload, path)


def write_decision_state_update_proposals(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    _write_json(payload, json_path)
    _write_text(render_decision_state_proposal_markdown(payload), markdown_path)


def write_decision_journal_validation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    _write_json(payload, json_path)
    _write_text(render_decision_journal_validation_markdown(payload), markdown_path)


def load_weekly_review_report(path: Path) -> dict[str, Any]:
    payload = _read_json_object(path)
    if _text(payload.get("report_type")) != "etf_weekly_review":
        raise DecisionJournalError(f"source weekly review is not etf_weekly_review: {path}")
    _validate_source_weekly_review_safety(payload)
    if not _records(payload.get("manual_review_actions")):
        raise DecisionJournalError("source weekly review has no manual_review_actions")
    return payload


def find_weekly_review_action(
    weekly_review_payload: Mapping[str, Any],
    action_item_id: str,
) -> dict[str, Any]:
    for action in _records(weekly_review_payload.get("manual_review_actions")):
        if _text(action.get("action_id")) == action_item_id:
            if action.get("requires_manual_review") is not True:
                raise DecisionJournalError("source action item must require manual review")
            if not _records(action.get("evidence")):
                raise DecisionJournalError("source action item must include evidence")
            return action
    raise DecisionJournalError(f"action_item_id not found in weekly review: {action_item_id}")


def validate_decision_entry_links(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    if _text(entry.get("source_section")) == "baseline_review":
        return _validate_baseline_review_entry_links(entry)
    issues: list[dict[str, Any]] = []
    weekly_path = Path(_text(entry.get("source_weekly_review")))
    linked_path = Path(_text(entry.get("linked_report")))
    if not weekly_path.exists():
        issues.append(_issue("source_weekly_review", "FAIL", f"missing {weekly_path}"))
    if not linked_path.exists():
        issues.append(_issue("linked_report", "FAIL", f"missing {linked_path}"))
    if issues:
        raise DecisionJournalError(_format_issues(issues))
    weekly_review = load_weekly_review_report(weekly_path)
    review_id = _text(weekly_review.get("review_id"))
    if _text(entry.get("review_id")) != review_id:
        issues.append(
            _issue("review_id", "FAIL", f"review_id does not match weekly review: {review_id}")
        )
    try:
        action = find_weekly_review_action(weekly_review, _text(entry.get("action_item_id")))
    except DecisionJournalError as exc:
        issues.append(_issue("action_item_id", "FAIL", str(exc)))
    else:
        if _text(entry.get("source_action_type")) and _text(
            entry.get("source_action_type")
        ) != _text(action.get("action_type")):
            issues.append(_issue("source_action_type", "FAIL", "source action type mismatch"))
    if issues:
        raise DecisionJournalError(_format_issues(issues))
    return issues


def _validate_baseline_review_entry_links(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    source_text = _text(entry.get("source_baseline_review_package"))
    if not source_text:
        source_text = _text(entry.get("source_weekly_review"))
    source_path = Path(source_text)
    linked_path = Path(_text(entry.get("linked_report")))
    if not source_path.exists():
        issues.append(_issue("source_baseline_review_package", "FAIL", f"missing {source_path}"))
    if not linked_path.exists():
        issues.append(_issue("linked_report", "FAIL", f"missing {linked_path}"))
    if issues:
        raise DecisionJournalError(_format_issues(issues))
    package = _read_json_object(source_path)
    if _text(package.get("report_type")) != "etf_baseline_review_package":
        issues.append(
            _issue(
                "source_baseline_review_package",
                "FAIL",
                "source package must be etf_baseline_review_package",
            )
        )
    if _text(package.get("review_package_id")) != _text(entry.get("review_id")):
        issues.append(_issue("review_id", "FAIL", "review_id does not match package ID"))
    if _text(package.get("candidate_id")) != _text(entry.get("linked_candidate")):
        issues.append(
            _issue("linked_candidate", "FAIL", "linked_candidate does not match package")
        )
    if issues:
        raise DecisionJournalError(_format_issues(issues))
    return issues


def validate_decision_journal_schema(journal: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if _text(journal.get("schema_version")) != DECISION_JOURNAL_SCHEMA_VERSION:
        issues.append(
            _issue(
                "schema_version",
                "FAIL",
                f"schema_version must be {DECISION_JOURNAL_SCHEMA_VERSION}",
            )
        )
    issues.extend(_safety_issues(journal, owner_id="journal"))
    for field in ("entries", "removed_entries"):
        if field in journal and not isinstance(journal.get(field), list):
            issues.append(_issue(field, "FAIL", f"{field} must be a list"))
    seen: set[str] = set()
    for index, entry in enumerate(_records(journal.get("entries"))):
        entry_issues = validate_decision_entry_schema(entry)
        for issue in entry_issues:
            issue["owner_id"] = _text(entry.get("decision_id"), f"entries[{index}]")
            issues.append(issue)
        decision_id = _text(entry.get("decision_id"))
        if decision_id in seen:
            issues.append(_issue("decision_id", "FAIL", f"duplicate decision_id: {decision_id}"))
        if decision_id:
            seen.add(decision_id)
    if issues:
        raise DecisionJournalError(_format_issues(issues))
    return issues


def validate_decision_entry(entry: Mapping[str, Any]) -> None:
    issues = validate_decision_entry_schema(entry)
    if issues:
        raise DecisionJournalError(_format_issues(issues))


def validate_decision_entry_schema(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for field in REQUIRED_DECISION_FIELDS:
        if field not in entry or _text(entry.get(field)) == "":
            issues.append(_issue(field, "FAIL", f"required field missing: {field}"))
    status = _text(entry.get("decision_status"))
    if status and status not in DECISION_STATUS_VALUES:
        issues.append(_issue("decision_status", "FAIL", f"unsupported decision_status: {status}"))
    confidence = _float_or_none(entry.get("confidence"))
    if confidence is None or confidence < 0.0 or confidence > 1.0:
        issues.append(
            _issue("confidence", "FAIL", "confidence must be numeric between 0.0 and 1.0")
        )
    for field, value in entry.items():
        if field in ACTION_LIKE_FIELDS and _contains_disallowed_action(value):
            issues.append(_issue(field, "FAIL", f"disallowed action found in {field}"))
    issues.extend(_safety_issues(entry, owner_id=_text(entry.get("decision_id"), "entry")))
    return issues


def allowed_decision_statuses() -> list[str]:
    return sorted(DECISION_STATUS_VALUES)


def disallowed_decision_actions() -> list[str]:
    return sorted(DISALLOWED_DECISION_ACTIONS)


def _decision_id(
    *,
    review_id: str,
    action_item_id: str,
    decision_status: str,
    created_at: datetime,
) -> str:
    basis = "|".join([review_id, action_item_id, decision_status, created_at.isoformat()])
    return "decision-" + sha256(basis.encode("utf-8")).hexdigest()[:12]


def _safety_issues(payload: Mapping[str, Any], *, owner_id: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for field, expected in DECISION_JOURNAL_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(
                _issue(
                    field,
                    "FAIL",
                    f"{owner_id} safety field {field} must be {expected!r}",
                )
            )
    return issues


def _validate_source_weekly_review_safety(payload: Mapping[str, Any]) -> None:
    for field, expected in DECISION_JOURNAL_SAFETY.items():
        if payload.get(field) != expected:
            raise DecisionJournalError(
                f"source weekly review safety field {field} must be {expected!r}"
            )


def _source_section_for_action(action: Mapping[str, Any]) -> str:
    source_module = _text(action.get("source_module"))
    action_type = _text(action.get("action_type"))
    if source_module == "etf_forward_dashboard" or "candidate" in action_type:
        return "shadow_candidate_review"
    if source_module == "ai_confirmation":
        return "ai_confirmation_review"
    if source_module == "satellite_replacement":
        return "satellite_replacement_review"
    if source_module in {"risk_watchlist", "weekly_review"}:
        return "risk_watchlist_constraints"
    return source_module or "weekly_review"


def _state_action_for_decision(decision_status: str) -> str:
    return {
        "accept_recommendation": "record_manual_acceptance_no_state_mutation",
        "reject_recommendation": "record_manual_rejection_no_state_mutation",
        "defer_decision": "defer_candidate_state_change",
        "continue_observation": "continue_observation",
        "mark_watch": "mark_watch",
        "archive_candidate_after_review": "archive_candidate_after_review",
        "start_new_experiment": "start_new_experiment",
        "request_more_data": "request_more_data",
    }.get(decision_status, "manual_review_required")


def _linked_candidates_and_reports(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for entry in entries:
        candidate = _text(entry.get("linked_candidate"), "UNKNOWN")
        row = grouped.setdefault(
            candidate,
            {"linked_candidate": candidate, "decision_count": 0, "linked_reports": set()},
        )
        row["decision_count"] += 1
        if _text(entry.get("linked_report")):
            row["linked_reports"].add(_text(entry.get("linked_report")))
        if _text(entry.get("source_weekly_review")):
            row["linked_reports"].add(_text(entry.get("source_weekly_review")))
    result = []
    for row in grouped.values():
        result.append(
            {
                "linked_candidate": row["linked_candidate"],
                "decision_count": row["decision_count"],
                "linked_reports": sorted(row["linked_reports"]),
            }
        )
    return sorted(result, key=lambda item: _text(item.get("linked_candidate")))


def _confidence_rationale_notes(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "decision_id": entry.get("decision_id"),
            "decision_status": entry.get("decision_status"),
            "confidence": entry.get("confidence"),
            "rationale": entry.get("rationale"),
            "linked_candidate": entry.get("linked_candidate"),
            "action_item_id": entry.get("action_item_id"),
        }
        for entry in entries
    ]


def _follow_up_tasks(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "decision_id": entry.get("decision_id"),
            "linked_candidate": entry.get("linked_candidate"),
            "follow_up_task": entry.get("follow_up_task"),
            "decision_status": entry.get("decision_status"),
        }
        for entry in entries
        if _text(entry.get("follow_up_task"))
    ]


def _audit_trail(
    entries: Sequence[Mapping[str, Any]],
    removed_entries: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for entry in [*entries, *removed_entries]:
        for event in _records(entry.get("audit_trail")):
            rows.append(
                {
                    "decision_id": entry.get("decision_id"),
                    "event": event.get("event"),
                    "timestamp": event.get("timestamp"),
                    "details": {
                        key: value
                        for key, value in event.items()
                        if key not in {"event", "timestamp"}
                    },
                }
            )
    return sorted(rows, key=lambda item: _text(item.get("timestamp")))


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "message": message,
            "details": dict(details or {}),
        }
    )


def _contains_unsafe_key(value: object) -> bool:
    unsafe_keys = {
        "production_weight_update",
        "broker_order",
        "broker_orders",
        "production_weights",
        "automatic_candidate_promotion",
        "automatic_candidate_rejection",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in unsafe_keys:
                return True
            if _contains_unsafe_key(child):
                return True
    elif isinstance(value, list):
        return any(_contains_unsafe_key(item) for item in value)
    return False


def _disallowed_action_validation_passes() -> bool:
    try:
        validate_decision_entry(
            {
                "review_id": "unit",
                "decision_id": "unit",
                "review_date": "2026-06-01",
                "source_weekly_review": "weekly.json",
                "action_item_id": "action",
                "human_decision": "place_order",
                "decision_status": "continue_observation",
                "rationale": "unit",
                "confidence": 0.5,
                "follow_up_task": "unit",
                "linked_candidate": "unit",
                "linked_report": "weekly.json",
                "created_at": "2026-06-02T00:00:00+00:00",
                **DECISION_JOURNAL_SAFETY,
            }
        )
    except DecisionJournalError:
        return True
    return False


def _contains_disallowed_action(value: object) -> bool:
    text = _text(value).lower().replace("-", "_").replace(" ", "_")
    return any(action in text for action in DISALLOWED_DECISION_ACTIONS)


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: object) -> list[str]:
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    if isinstance(value, tuple):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    return [text] if text else []


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DecisionJournalError(f"missing JSON file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise DecisionJournalError(f"invalid JSON file: {path}") from exc
    if not isinstance(payload, Mapping):
        raise DecisionJournalError(f"JSON root must be an object: {path}")
    return dict(payload)


def _write_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _issue(check_id: str, status: str, message: str) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": status,
        "message": message,
    }


def _format_issues(issues: Sequence[Mapping[str, Any]]) -> str:
    return "; ".join(_text(issue.get("message"), "validation issue") for issue in issues)
