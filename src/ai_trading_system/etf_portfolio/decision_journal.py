from __future__ import annotations

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


def _contains_disallowed_action(value: object) -> bool:
    text = _text(value).lower().replace("-", "_").replace(" ", "_")
    return any(action in text for action in DISALLOWED_DECISION_ACTIONS)


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


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


def _issue(check_id: str, status: str, message: str) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": status,
        "message": message,
    }


def _format_issues(issues: Sequence[Mapping[str, Any]]) -> str:
    return "; ".join(_text(issue.get("message"), "validation issue") for issue in issues)
