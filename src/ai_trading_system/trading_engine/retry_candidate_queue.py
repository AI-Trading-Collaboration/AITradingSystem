from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "retry_candidate_queue"
TASK_ID = "TRADING-037"
TASK_NAME = "Retry Candidate Queue / Manual Approval Gate"
SOURCE_TASK_ID = "TRADING-036"
MODE = "read_only"
PRODUCTION_EFFECT_NONE = "none"

QUEUE_EMPTY = "EMPTY"
QUEUE_PENDING_APPROVAL = "PENDING_APPROVAL"
QUEUE_BLOCKED = "BLOCKED"
QUEUE_SAFETY_BLOCKED = "SAFETY_BLOCKED"
QUEUE_SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"

APPROVAL_NOT_REQUESTED = "NOT_REQUESTED"
RETRY_PENDING_APPROVAL = "PENDING_APPROVAL"
RETRY_BLOCKED = "BLOCKED"

CATEGORY_TRANSIENT = "TRANSIENT_DELIVERY_FAILURE"
CATEGORY_CONFIGURATION = "CONFIGURATION_FAILURE"
CATEGORY_SAFETY_BLOCKED = "SAFETY_BLOCKED"
CATEGORY_CONTENT_MISMATCH = "CONTENT_MISMATCH"
CATEGORY_MISSING_ARTIFACT = "MISSING_ARTIFACT"
CATEGORY_UNKNOWN = "UNKNOWN"

CATEGORY_ORDER = (
    CATEGORY_TRANSIENT,
    CATEGORY_CONFIGURATION,
    CATEGORY_SAFETY_BLOCKED,
    CATEGORY_CONTENT_MISMATCH,
    CATEGORY_MISSING_ARTIFACT,
    CATEGORY_UNKNOWN,
)
BLOCKED_CATEGORIES = {
    CATEGORY_CONFIGURATION,
    CATEGORY_SAFETY_BLOCKED,
    CATEGORY_CONTENT_MISMATCH,
    CATEGORY_MISSING_ARTIFACT,
    CATEGORY_UNKNOWN,
}

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "retry_candidate_queue"


def default_retry_candidate_queue_output_dir(project_root: Path = REPO_ROOT) -> Path:
    return project_root / "outputs" / "retry_candidate_queue"


def default_retry_candidate_queue_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"retry_candidate_queue_{as_of.isoformat()}.json"


def default_failure_classification_output_dir(project_root: Path = REPO_ROOT) -> Path:
    return project_root / "outputs" / "notification_delivery_failure_classification"


def write_retry_candidate_queue(
    *,
    as_of: date | None = None,
    classification_report_path: Path | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    project_root: Path = REPO_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    project_root = _normalize_path(project_root)
    output_dir = _resolve_project_path(project_root, output_dir)
    source_path = _resolve_source_path(
        project_root=project_root,
        as_of=as_of,
        classification_report_path=classification_report_path,
        generated_at=generated,
    )
    source_payload, source_available, source_parse_status, source_error = _read_source_json(
        source_path
    )
    queue_date = _queue_date(
        as_of=as_of,
        source_path=source_path,
        source_payload=source_payload,
        generated_at=generated,
    )
    json_path = default_retry_candidate_queue_json_path(output_dir, queue_date)
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")

    payload = build_retry_candidate_queue(
        as_of=queue_date,
        source_path=source_path,
        source_payload=source_payload,
        source_available=source_available,
        source_parse_status=source_parse_status,
        source_error=source_error,
        output_json_path=json_path,
        output_markdown_path=markdown_path,
        run_log_path=log_path,
        generated_at=generated,
    )
    _write_json(json_path, payload)
    _write_text(markdown_path, render_retry_candidate_queue_markdown(payload))
    _write_text(log_path, render_retry_candidate_queue_log(payload))
    return payload


def build_retry_candidate_queue(
    *,
    as_of: date,
    source_path: Path,
    source_payload: dict[str, Any],
    source_available: bool,
    source_parse_status: str,
    source_error: str,
    output_json_path: Path | None = None,
    output_markdown_path: Path | None = None,
    run_log_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    source = _source_classification(
        source_path=source_path,
        source_payload=source_payload,
        source_available=source_available,
        source_parse_status=source_parse_status,
        source_error=source_error,
    )
    candidates, blocked_items = _build_queue_items(
        as_of=as_of,
        source_path=source_path,
        source_payload=source_payload,
        source_parse_status=source_parse_status,
    )
    queue_status = _queue_status(
        source_parse_status=source_parse_status,
        source_payload=source_payload,
        candidates=candidates,
        blocked_items=blocked_items,
    )
    summary = _queue_summary(queue_status, candidates, blocked_items)
    approval_gate = {
        "approval_required": True,
        "approval_status": APPROVAL_NOT_REQUESTED,
        "approved_by": None,
        "approved_at": None,
        "approval_note": None,
        "retry_execution_allowed": False,
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "retry_candidate_queue_only": True,
        "read_only": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "external_delivery_executed": False,
        "retry_executed": False,
        "delivery_state_mutated": False,
        "approval_state_modified": False,
        "production_parameters_modified": False,
        "notification_delivery_failure_classification_executed_by_queue": False,
        "notification_delivery_audit_executed_by_queue": False,
        "notification_draft_executed_by_queue": False,
        "delivery_preflight_executed_by_queue": False,
        "draft_dispatch_executed_by_queue": False,
        "operator_brief_executed_by_queue": False,
        "pipelines_executed_by_queue": False,
        "data_downloaded_by_queue": False,
        "apply_executed_by_queue": False,
        "rollback_executed_by_queue": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": TASK_ID,
            "task_name": TASK_NAME,
            "mode": MODE,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
            "generated_at": _isoformat(generated),
            "schema_version": SCHEMA_VERSION,
        },
        "source_classification": source,
        "queue_summary": summary,
        "candidate_queue": candidates,
        "blocked_items": blocked_items,
        "approval_gate": approval_gate,
        "recommended_actions": _recommended_actions(
            queue_status=queue_status,
            source_payload=source_payload,
            source_error=source_error,
        ),
        "safety_invariants": {
            "read_only": True,
            "no_external_delivery": True,
            "no_retry_execution": True,
            "no_state_mutation": True,
            "no_production_parameter_change": True,
            "dashboard_read_only": True,
        },
        "output_artifacts": {
            "retry_candidate_queue_json": {
                "path": "" if output_json_path is None else str(output_json_path)
            },
            "retry_candidate_queue_markdown": {
                "path": "" if output_markdown_path is None else str(output_markdown_path)
            },
            "run_log": {"path": "" if run_log_path is None else str(run_log_path)},
        },
    }
    _assert_safety_invariants(payload)
    return payload


def find_latest_notification_delivery_failure_classification(
    *,
    project_root: Path = REPO_ROOT,
    as_of: date | None = None,
) -> Path | None:
    classification_root = default_failure_classification_output_dir(_normalize_path(project_root))
    if not classification_root.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in classification_root.glob("notification_delivery_failure_classification_*.json"):
        parsed = _classification_date_from_path(path)
        if parsed is None:
            continue
        if as_of is not None and parsed > as_of:
            continue
        candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def should_fail_cli(
    payload: dict[str, Any],
    *,
    fail_on_safety_blocked: bool = False,
) -> bool:
    if not fail_on_safety_blocked:
        return False
    summary = _mapping(payload.get("queue_summary"))
    return summary.get("queue_status") == QUEUE_SAFETY_BLOCKED


def render_retry_candidate_queue_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    source = _mapping(payload.get("source_classification"))
    summary = _mapping(payload.get("queue_summary"))
    candidates = _items(payload.get("candidate_queue"))
    blocked_items = _items(payload.get("blocked_items"))
    approval = _mapping(payload.get("approval_gate"))
    safety = _mapping(payload.get("safety_invariants"))
    queue_status = _string_value(summary.get("queue_status")) or QUEUE_SOURCE_UNAVAILABLE

    lines = [
        "# Retry Candidate Queue / Manual Approval Gate",
        "",
        "## Status Banner",
        "",
        _status_banner(queue_status),
        "",
    ]
    if queue_status == QUEUE_SAFETY_BLOCKED:
        lines.extend(
            [
                "CRITICAL: Safety blocked. Retry execution is not allowed.",
                "",
            ]
        )
    lines.extend(
        [
            "## Source Classification",
            "",
            f"- Source task: `{source.get('task_id', SOURCE_TASK_ID)}`",
            f"- Source report: `{source.get('classification_report_path', '')}`",
            f"- Source status: `{source.get('overall_status', 'UNKNOWN')}`",
            f"- Highest severity: `{source.get('highest_severity', 'UNKNOWN')}`",
            f"- Source parse status: `{source.get('source_parse_status', 'UNKNOWN')}`",
            "",
            "## Queue Summary",
            "",
            f"- Queue status: `{summary.get('queue_status', QUEUE_SOURCE_UNAVAILABLE)}`",
            f"- Total candidates: `{summary.get('total_candidates', 0)}`",
            f"- Blocked items: `{summary.get('blocked_candidates', 0)}`",
            f"- Manual review required: " f"`{_bool_text(summary.get('manual_review_required'))}`",
            f"- Has retryable candidates: "
            f"`{_bool_text(summary.get('has_retryable_candidates'))}`",
            f"- Safe to execute retry: " f"`{_bool_text(summary.get('safe_to_execute_retry'))}`",
            "",
            "## Candidate Queue",
            "",
        ]
    )
    if not candidates:
        lines.extend(["No retry candidates.", ""])
    else:
        for index, candidate in enumerate(candidates, start=1):
            lines.extend(
                [
                    f"### Candidate {index:03d}",
                    "",
                    f"- Candidate ID: `{candidate.get('candidate_id', '')}`",
                    f"- Source category: `{candidate.get('source_category', '')}`",
                    f"- Severity: `{candidate.get('severity', '')}`",
                    f"- Retryable: `{_bool_text(candidate.get('retryable'))}`",
                    "- Requires manual review: "
                    f"`{_bool_text(candidate.get('requires_manual_review'))}`",
                    f"- Approval required: " f"`{_bool_text(candidate.get('approval_required'))}`",
                    f"- Retry status: `{candidate.get('retry_status', '')}`",
                    "- Retry blockers:",
                    *_markdown_list(_strings(candidate.get("retry_blockers"))),
                    "",
                ]
            )
    lines.extend(["## Blocked Items", ""])
    if not blocked_items:
        lines.extend(["No blocked items.", ""])
    else:
        for index, item in enumerate(blocked_items, start=1):
            lines.extend(
                [
                    f"### Blocked Item {index:03d}",
                    "",
                    f"- Blocked item ID: `{item.get('blocked_item_id', '')}`",
                    f"- Source category: `{item.get('source_category', '')}`",
                    f"- Severity: `{item.get('severity', '')}`",
                    f"- Retryable: `{_bool_text(item.get('retryable'))}`",
                    "- Requires manual review: "
                    f"`{_bool_text(item.get('requires_manual_review'))}`",
                    f"- Retry status: `{item.get('retry_status', '')}`",
                    "- Retry blockers:",
                    *_markdown_list(_strings(item.get("retry_blockers"))),
                    "",
                ]
            )
    lines.extend(
        [
            "## Approval Gate",
            "",
            f"- Approval required: `{_bool_text(approval.get('approval_required'))}`",
            f"- Approval status: `{approval.get('approval_status', APPROVAL_NOT_REQUESTED)}`",
            "- Retry execution allowed: "
            f"`{_bool_text(approval.get('retry_execution_allowed'))}`",
            "",
            "## Recommended Actions",
            "",
            *_markdown_list(_strings(payload.get("recommended_actions"))),
            "",
            "## Safety Invariants",
            "",
            f"- Read only: `{_bool_text(safety.get('read_only'))}`",
            f"- No external delivery: `{_bool_text(safety.get('no_external_delivery'))}`",
            f"- No retry execution: `{_bool_text(safety.get('no_retry_execution'))}`",
            f"- No state mutation: `{_bool_text(safety.get('no_state_mutation'))}`",
            "- No production parameter change: "
            f"`{_bool_text(safety.get('no_production_parameter_change'))}`",
            f"- Dashboard read only: `{_bool_text(safety.get('dashboard_read_only'))}`",
            "",
            "## Metadata",
            "",
            f"- Task ID: `{metadata.get('task_id', TASK_ID)}`",
            f"- Mode: `{metadata.get('mode', MODE)}`",
            f"- Production effect: `{metadata.get('production_effect', PRODUCTION_EFFECT_NONE)}`",
            f"- Manual review only: `{_bool_text(metadata.get('manual_review_only'))}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_retry_candidate_queue_log(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    source = _mapping(payload.get("source_classification"))
    summary = _mapping(payload.get("queue_summary"))
    approval = _mapping(payload.get("approval_gate"))
    outputs = _mapping(payload.get("output_artifacts"))
    return "\n".join(
        [
            f"task_id={metadata.get('task_id', TASK_ID)}",
            f"generated_at={metadata.get('generated_at', '')}",
            f"source_parse_status={source.get('source_parse_status', 'UNKNOWN')}",
            f"source_overall_status={source.get('overall_status', 'UNKNOWN')}",
            f"source_highest_severity={source.get('highest_severity', 'UNKNOWN')}",
            f"queue_status={summary.get('queue_status', QUEUE_SOURCE_UNAVAILABLE)}",
            f"total_candidates={summary.get('total_candidates', 0)}",
            f"blocked_candidates={summary.get('blocked_candidates', 0)}",
            f"manual_review_required={_bool_text(summary.get('manual_review_required'))}",
            f"has_retryable_candidates={_bool_text(summary.get('has_retryable_candidates'))}",
            f"safe_to_execute_retry={_bool_text(summary.get('safe_to_execute_retry'))}",
            f"approval_status={approval.get('approval_status', APPROVAL_NOT_REQUESTED)}",
            f"retry_execution_allowed={_bool_text(approval.get('retry_execution_allowed'))}",
            "production_effect=none",
            "manual_review_only=true",
            "read_only=true",
            "no_external_delivery=true",
            "no_retry_execution=true",
            "no_state_mutation=true",
            "no_production_parameter_change=true",
            "dashboard_read_only=true",
            "retry_candidate_queue_json="
            f"{_mapping(outputs.get('retry_candidate_queue_json')).get('path', '')}",
            "retry_candidate_queue_markdown="
            f"{_mapping(outputs.get('retry_candidate_queue_markdown')).get('path', '')}",
            f"run_log={_mapping(outputs.get('run_log')).get('path', '')}",
            "",
        ]
    )


def _resolve_source_path(
    *,
    project_root: Path,
    as_of: date | None,
    classification_report_path: Path | None,
    generated_at: datetime,
) -> Path:
    if classification_report_path is not None:
        return _resolve_project_path(project_root, classification_report_path)
    latest = find_latest_notification_delivery_failure_classification(
        project_root=project_root,
        as_of=as_of,
    )
    if latest is not None:
        return latest
    fallback_date = as_of or generated_at.date()
    return (
        project_root
        / "outputs"
        / "notification_delivery_failure_classification"
        / f"notification_delivery_failure_classification_{fallback_date.isoformat()}.json"
    )


def _read_source_json(path: Path) -> tuple[dict[str, Any], bool, str, str]:
    if not path.exists():
        return {}, False, "MISSING", f"Source classification report not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, True, "MALFORMED_JSON", f"Source classification JSON malformed: {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, True, "INVALID_SHAPE", f"Source classification JSON must be an object: {path}"
    if payload.get("report_type") != "notification_delivery_failure_classification":
        return (
            {},
            True,
            "INVALID_REPORT_TYPE",
            f"Source classification report_type is not TRADING-036: {path}",
        )
    return payload, True, "OK", ""


def _queue_date(
    *,
    as_of: date | None,
    source_path: Path,
    source_payload: dict[str, Any],
    generated_at: datetime,
) -> date:
    if as_of is not None:
        return as_of
    payload_date = _parse_iso_date(_string_value(source_payload.get("date")))
    if payload_date is not None:
        return payload_date
    path_date = _classification_date_from_path(source_path)
    if path_date is not None:
        return path_date
    return generated_at.date()


def _source_classification(
    *,
    source_path: Path,
    source_payload: dict[str, Any],
    source_available: bool,
    source_parse_status: str,
    source_error: str,
) -> dict[str, Any]:
    summary = _mapping(source_payload.get("classification_summary"))
    return {
        "task_id": SOURCE_TASK_ID,
        "classification_report_path": str(source_path),
        "overall_status": _string_value(summary.get("overall_status"))
        or ("MISSING" if source_parse_status == "MISSING" else "UNKNOWN"),
        "highest_severity": _string_value(summary.get("highest_severity")) or "UNKNOWN",
        "source_available": source_available,
        "source_parse_status": source_parse_status,
        "source_error": source_error,
    }


def _build_queue_items(
    *,
    as_of: date,
    source_path: Path,
    source_payload: dict[str, Any],
    source_parse_status: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if source_parse_status != "OK":
        return [], []
    categories = _mapping(source_payload.get("failure_categories"))
    candidate_queue: list[dict[str, Any]] = []
    blocked_items: list[dict[str, Any]] = []
    source_item_counter = 0
    candidate_counter = 0
    blocked_counter = 0

    for category in CATEGORY_ORDER:
        record = _mapping(categories.get(category))
        items = _category_items(record)
        if not items:
            continue
        for item in items:
            source_item_counter += 1
            source_item_id = _source_item_id(item, source_item_counter)
            if record.get("retryable") is True and category not in BLOCKED_CATEGORIES:
                candidate_counter += 1
                candidate_queue.append(
                    _candidate_item(
                        as_of=as_of,
                        sequence=candidate_counter,
                        source_path=source_path,
                        category=category,
                        record=record,
                        item=item,
                        source_item_id=source_item_id,
                    )
                )
                continue
            blocked_counter += 1
            blocked_items.append(
                _blocked_item(
                    as_of=as_of,
                    sequence=blocked_counter,
                    source_path=source_path,
                    category=category,
                    record=record,
                    item=item,
                    source_item_id=source_item_id,
                )
            )
    return candidate_queue, blocked_items


def _candidate_item(
    *,
    as_of: date,
    sequence: int,
    source_path: Path,
    category: str,
    record: dict[str, Any],
    item: dict[str, Any],
    source_item_id: str,
) -> dict[str, Any]:
    reason = _string_value(item.get("reason")) or (
        "Transient delivery failure detected by TRADING-036."
    )
    return {
        "candidate_id": f"retry_candidate_{as_of.isoformat()}_{sequence:03d}",
        "source_category": category,
        "source_item_id": source_item_id,
        "severity": _string_value(record.get("severity")) or "WARN",
        "retryable": True,
        "requires_manual_review": True,
        "approval_required": True,
        "retry_status": RETRY_PENDING_APPROVAL,
        "retry_reason": reason,
        "retry_blockers": [],
        "source_evidence": _source_evidence(source_path, category, item),
    }


def _blocked_item(
    *,
    as_of: date,
    sequence: int,
    source_path: Path,
    category: str,
    record: dict[str, Any],
    item: dict[str, Any],
    source_item_id: str,
) -> dict[str, Any]:
    blockers = _retry_blockers(category, item)
    return {
        "blocked_item_id": f"retry_blocked_{as_of.isoformat()}_{sequence:03d}",
        "source_category": category,
        "source_item_id": source_item_id,
        "severity": _string_value(record.get("severity")) or "ERROR",
        "retryable": False,
        "requires_manual_review": True,
        "approval_required": True,
        "retry_status": RETRY_BLOCKED,
        "block_reason": blockers[0] if blockers else f"{category} is not retryable.",
        "retry_blockers": blockers,
        "source_evidence": _source_evidence(source_path, category, item),
    }


def _source_evidence(source_path: Path, category: str, item: dict[str, Any]) -> dict[str, Any]:
    return {
        "classification_report_path": str(source_path),
        "failure_category": category,
        "source_audit_status": _string_value(item.get("source_audit_status")),
        "source_audit_path": _string_value(item.get("source_audit_path")),
        "source_reason": _string_value(item.get("reason")),
        "source_details": _strings(item.get("details")),
    }


def _queue_status(
    *,
    source_parse_status: str,
    source_payload: dict[str, Any],
    candidates: list[dict[str, Any]],
    blocked_items: list[dict[str, Any]],
) -> str:
    if source_parse_status != "OK":
        return QUEUE_SOURCE_UNAVAILABLE
    categories = _mapping(source_payload.get("failure_categories"))
    safety = _mapping(categories.get(CATEGORY_SAFETY_BLOCKED))
    summary = _mapping(source_payload.get("classification_summary"))
    if (
        _int_value(safety.get("count")) > 0
        or summary.get("overall_status") == "CRITICAL"
        or summary.get("highest_severity") == "CRITICAL"
    ):
        return QUEUE_SAFETY_BLOCKED
    if candidates:
        return QUEUE_PENDING_APPROVAL
    if blocked_items:
        return QUEUE_BLOCKED
    return QUEUE_EMPTY


def _queue_summary(
    queue_status: str,
    candidates: list[dict[str, Any]],
    blocked_items: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "queue_status": queue_status,
        "total_candidates": len(candidates),
        "approved_candidates": 0,
        "blocked_candidates": len(blocked_items),
        "manual_review_required": queue_status
        in {
            QUEUE_PENDING_APPROVAL,
            QUEUE_BLOCKED,
            QUEUE_SAFETY_BLOCKED,
            QUEUE_SOURCE_UNAVAILABLE,
        },
        "has_retryable_candidates": bool(candidates),
        "safe_to_execute_retry": False,
    }


def _recommended_actions(
    *,
    queue_status: str,
    source_payload: dict[str, Any],
    source_error: str,
) -> list[str]:
    source_actions = _strings(source_payload.get("recommended_actions"))
    if queue_status == QUEUE_EMPTY:
        return ["No notification delivery failures detected; no retry is required."]
    if queue_status == QUEUE_PENDING_APPROVAL:
        return [
            "Review retry candidates and record explicit manual approval before any retry dry-run.",
            "Do not execute real retry from TRADING-037.",
            *source_actions,
        ]
    if queue_status == QUEUE_SAFETY_BLOCKED:
        return [
            "CRITICAL: Safety blocked. Retry execution is not allowed.",
            "Resolve the safety-blocked TRADING-036 finding before considering retry.",
            *source_actions,
        ]
    if queue_status == QUEUE_SOURCE_UNAVAILABLE:
        action = (
            source_error
            or "Restore or regenerate the TRADING-036 classification report before retry review."
        )
        return [
            action,
            "Do not infer retry eligibility from a missing or malformed source report.",
        ]
    return [
        "Resolve blocked items before considering retry.",
        "Manual review is required; TRADING-037 does not authorize retry execution.",
        *source_actions,
    ]


def _category_items(record: dict[str, Any]) -> list[dict[str, Any]]:
    items = _items(record.get("items"))
    count = max(_int_value(record.get("count")), len(items))
    if count <= 0:
        return []
    if items:
        missing = count - len(items)
        return [
            *items,
            *(
                {"reason": "TRADING-036 category count without item details.", "details": []}
                for _ in range(max(0, missing))
            ),
        ]
    return [
        {"reason": "TRADING-036 category count without item details.", "details": []}
        for _ in range(count)
    ]


def _retry_blockers(category: str, item: dict[str, Any]) -> list[str]:
    blockers = list(_strings(item.get("details")))
    reason = _string_value(item.get("reason"))
    if reason and reason not in blockers:
        blockers.insert(0, reason)
    if category == CATEGORY_SAFETY_BLOCKED:
        blockers.append("Safety blocked by TRADING-036 classification.")
    elif category in BLOCKED_CATEGORIES:
        blockers.append(f"{category} is not retryable without manual remediation.")
    return _unique_strings(blockers)


def _source_item_id(item: dict[str, Any], sequence: int) -> str:
    return (
        _string_value(item.get("source_item_id"))
        or _string_value(item.get("item_id"))
        or f"delivery_failure_{sequence:03d}"
    )


def _status_banner(queue_status: str) -> str:
    if queue_status == QUEUE_EMPTY:
        return "EMPTY: No retry candidates."
    if queue_status == QUEUE_PENDING_APPROVAL:
        return "PENDING_APPROVAL: Retry candidates exist but require manual approval."
    if queue_status == QUEUE_BLOCKED:
        return "BLOCKED: Retry is blocked by non-retryable failures."
    if queue_status == QUEUE_SAFETY_BLOCKED:
        return "SAFETY_BLOCKED: Retry execution is prohibited."
    return "SOURCE_UNAVAILABLE: Source classification report is missing or malformed."


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-037 production_effect must be none")
    true_fields = ("manual_review_only", "retry_candidate_queue_only", "read_only")
    for field in true_fields:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-037 {field} must be true")
    false_fields = (
        "email_sent",
        "gmail_draft_created",
        "gmail_draft_modified",
        "slack_sent",
        "discord_sent",
        "webhook_called",
        "mobile_push_sent",
        "external_delivery_executed",
        "retry_executed",
        "delivery_state_mutated",
        "approval_state_modified",
        "production_parameters_modified",
        "notification_delivery_failure_classification_executed_by_queue",
        "notification_delivery_audit_executed_by_queue",
        "notification_draft_executed_by_queue",
        "delivery_preflight_executed_by_queue",
        "draft_dispatch_executed_by_queue",
        "operator_brief_executed_by_queue",
        "pipelines_executed_by_queue",
        "data_downloaded_by_queue",
        "apply_executed_by_queue",
        "rollback_executed_by_queue",
        "broker_execution",
        "replay_execution",
        "trading_execution",
    )
    for field in false_fields:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-037 {field} must be false")
    safety = _mapping(payload.get("safety_invariants"))
    for field in (
        "read_only",
        "no_external_delivery",
        "no_retry_execution",
        "no_state_mutation",
        "no_production_parameter_change",
        "dashboard_read_only",
    ):
        if safety.get(field) is not True:
            raise ValueError(f"TRADING-037 safety_invariants.{field} must be true")
    approval = _mapping(payload.get("approval_gate"))
    if approval.get("retry_execution_allowed") is not False:
        raise ValueError("TRADING-037 approval_gate.retry_execution_allowed must be false")


def _classification_date_from_path(path: Path) -> date | None:
    raw_date = path.stem.removeprefix("notification_delivery_failure_classification_")
    return _parse_iso_date(raw_date)


def _resolve_project_path(project_root: Path, value: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path.resolve(strict=False)
    return (project_root / path).resolve(strict=False)


def _normalize_path(path: Path) -> Path:
    value = Path(path)
    if value.is_absolute():
        return value.resolve(strict=False)
    return (Path.cwd() / value).resolve(strict=False)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _items(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _string_value(value: object) -> str:
    return value if isinstance(value, str) else ""


def _int_value(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _markdown_list(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _bool_text(value: object) -> str:
    return "true" if value is True else "false"


def _isoformat(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")
