from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "notification_delivery_failure_classification"
TASK_ID = "TRADING-036"
TASK_NAME = "Notification Delivery Failure Classification / Retry Readiness"
SOURCE_TASK_ID = "TRADING-035"
MODE = "read_only"
PRODUCTION_EFFECT_NONE = "none"

AUDIT_PASS = "PASS"
AUDIT_PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"
AUDIT_INCOMPLETE = "INCOMPLETE"
AUDIT_MISMATCH = "MISMATCH"
AUDIT_SAFETY_BLOCKED = "SAFETY_BLOCKED"

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_ERROR = "ERROR"
STATUS_CRITICAL = "CRITICAL"
STATUS_UNKNOWN = "UNKNOWN"

SEVERITY_NONE = "NONE"
SEVERITY_WARN = "WARN"
SEVERITY_ERROR = "ERROR"
SEVERITY_CRITICAL = "CRITICAL"

CATEGORY_TRANSIENT = "TRANSIENT_DELIVERY_FAILURE"
CATEGORY_CONFIGURATION = "CONFIGURATION_FAILURE"
CATEGORY_SAFETY_BLOCKED = "SAFETY_BLOCKED"
CATEGORY_CONTENT_MISMATCH = "CONTENT_MISMATCH"
CATEGORY_MISSING_ARTIFACT = "MISSING_ARTIFACT"
CATEGORY_UNKNOWN = "UNKNOWN"

CATEGORY_DEFINITIONS: tuple[tuple[str, str, bool, bool], ...] = (
    (CATEGORY_TRANSIENT, SEVERITY_WARN, True, False),
    (CATEGORY_CONFIGURATION, SEVERITY_ERROR, False, True),
    (CATEGORY_SAFETY_BLOCKED, SEVERITY_CRITICAL, False, True),
    (CATEGORY_CONTENT_MISMATCH, SEVERITY_ERROR, False, True),
    (CATEGORY_MISSING_ARTIFACT, SEVERITY_ERROR, False, True),
    (CATEGORY_UNKNOWN, SEVERITY_WARN, False, True),
)

CONFIGURATION_REASON_TOKENS = (
    "channel config",
    "recipient config",
    "notification target",
    "target config",
    "channel_readiness",
    "target_ref",
    "recipient",
)
TRANSIENT_REASON_TOKENS = (
    "timeout",
    "temporar",
    "rate limit",
    "429",
    "502",
    "503",
    "504",
    "network",
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "notification_delivery_failure_classification"


def default_failure_classification_output_dir(project_root: Path = REPO_ROOT) -> Path:
    return project_root / "outputs" / "notification_delivery_failure_classification"


def default_delivery_audit_roots(project_root: Path = REPO_ROOT) -> tuple[Path, Path]:
    return (
        project_root / "data" / "derived" / "operator_briefs" / "notifications" / "delivery_audit",
        project_root / "outputs" / "notification_delivery_audit_summary",
    )


def write_notification_delivery_failure_classification(
    *,
    as_of: date | None = None,
    audit_summary_path: Path | None = None,
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
        audit_summary_path=audit_summary_path,
        generated_at=generated,
    )
    source_payload, source_available, source_parse_status, source_error = _read_source_json(
        source_path
    )
    classification_date = _classification_date(
        as_of=as_of,
        source_path=source_path,
        source_payload=source_payload,
        generated_at=generated,
    )
    json_path = default_failure_classification_json_path(output_dir, classification_date)
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")

    payload = build_notification_delivery_failure_classification(
        as_of=classification_date,
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
    write_json_atomic(json_path, payload, sort_keys=False)
    write_text_atomic(
        markdown_path, render_notification_delivery_failure_classification_markdown(payload)
    )
    write_text_atomic(log_path, render_notification_delivery_failure_classification_log(payload))
    return payload


def build_notification_delivery_failure_classification(
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
    audit_status = _source_audit_status(source_payload, source_parse_status)
    audit_generated_at = _source_generated_at(source_payload)
    reasons = _source_reasons(
        source_payload=source_payload,
        source_available=source_available,
        source_parse_status=source_parse_status,
        source_error=source_error,
    )
    categories = _empty_failure_categories()
    _classify_into_categories(
        categories=categories,
        audit_status=audit_status,
        source_available=source_available,
        source_parse_status=source_parse_status,
        source_path=source_path,
        reasons=reasons,
    )
    summary = _classification_summary(categories, audit_status)
    retry_readiness = _retry_readiness(categories, summary, reasons)
    recommended_actions = _recommended_actions(
        overall_status=_string_value(summary.get("overall_status")),
        audit_status=audit_status,
        categories=categories,
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_delivery_failure_classification_only": True,
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
        "production_parameters_modified": False,
        "notification_delivery_audit_executed_by_classifier": False,
        "notification_draft_executed_by_classifier": False,
        "delivery_preflight_executed_by_classifier": False,
        "draft_dispatch_executed_by_classifier": False,
        "operator_brief_executed_by_classifier": False,
        "pipelines_executed_by_classifier": False,
        "data_downloaded_by_classifier": False,
        "apply_executed_by_classifier": False,
        "rollback_executed_by_classifier": False,
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
        "source_audit": {
            "task_id": SOURCE_TASK_ID,
            "audit_summary_path": str(source_path),
            "audit_status": audit_status,
            "audit_generated_at": audit_generated_at,
            "source_available": source_available,
            "source_parse_status": source_parse_status,
            "source_error": source_error,
        },
        "classification_summary": summary,
        "failure_categories": categories,
        "retry_readiness": retry_readiness,
        "recommended_actions": recommended_actions,
        "safety_invariants": {
            "read_only": True,
            "no_external_delivery": True,
            "no_state_mutation": True,
            "no_production_parameter_change": True,
            "dashboard_read_only": True,
        },
        "output_artifacts": {
            "classification_json": {
                "path": "" if output_json_path is None else str(output_json_path)
            },
            "classification_markdown": {
                "path": "" if output_markdown_path is None else str(output_markdown_path)
            },
            "run_log": {"path": "" if run_log_path is None else str(run_log_path)},
        },
    }
    _assert_safety_invariants(payload)
    return payload


def default_failure_classification_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"notification_delivery_failure_classification_{as_of.isoformat()}.json"


def find_latest_notification_delivery_audit_summary(
    *,
    project_root: Path = REPO_ROOT,
    as_of: date | None = None,
) -> Path | None:
    project_root = _normalize_path(project_root)
    candidates: list[tuple[date, Path]] = []
    for root in default_delivery_audit_roots(project_root):
        if not root.exists():
            continue
        for path in root.glob("notification_delivery_audit_summary_*.json"):
            parsed = _audit_date_from_path(path)
            if parsed is None:
                continue
            if as_of is not None and parsed > as_of:
                continue
            candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def should_fail_cli(payload: dict[str, Any], *, fail_on_critical: bool = False) -> bool:
    if not fail_on_critical:
        return False
    summary = _mapping(payload.get("classification_summary"))
    return (
        summary.get("overall_status") == STATUS_CRITICAL
        or summary.get("highest_severity") == SEVERITY_CRITICAL
    )


def render_notification_delivery_failure_classification_markdown(
    payload: dict[str, Any],
) -> str:
    metadata = _mapping(payload.get("metadata"))
    source = _mapping(payload.get("source_audit"))
    summary = _mapping(payload.get("classification_summary"))
    categories = _mapping(payload.get("failure_categories"))
    retry = _mapping(payload.get("retry_readiness"))
    safety = _mapping(payload.get("safety_invariants"))
    banner = _status_banner(_string_value(summary.get("overall_status")))
    lines = [
        "# Notification Delivery Failure Classification",
        "",
        "## Status Banner",
        "",
        banner,
        "",
    ]
    if summary.get("overall_status") == STATUS_CRITICAL:
        lines.extend(
            [
                "SAFETY BLOCKED: manual review required before any retry or delivery action.",
                "",
            ]
        )
    lines.extend(
        [
            "## Source Audit",
            "",
            f"- Source task: `{source.get('task_id', SOURCE_TASK_ID)}`",
            f"- Source audit status: `{source.get('audit_status', STATUS_UNKNOWN)}`",
            f"- Source audit artifact: `{source.get('audit_summary_path', '')}`",
            f"- Source parse status: `{source.get('source_parse_status', STATUS_UNKNOWN)}`",
            "",
            "## Classification Summary",
            "",
            f"- Overall status: `{summary.get('overall_status', STATUS_UNKNOWN)}`",
            f"- Total failures: `{summary.get('total_failures', 0)}`",
            f"- Highest severity: `{summary.get('highest_severity', SEVERITY_NONE)}`",
            f"- Requires manual review: `{_bool_text(summary.get('requires_manual_review'))}`",
            f"- Safe to retry: `{_bool_text(summary.get('safe_to_retry'))}`",
            f"- Blocks notification chain: "
            f"`{_bool_text(summary.get('blocks_notification_chain'))}`",
            "",
            "## Failure Categories",
            "",
        ]
    )
    for category, _, _, _ in CATEGORY_DEFINITIONS:
        record = _mapping(categories.get(category))
        lines.extend(
            [
                f"### {category}",
                "",
                f"- Count: `{record.get('count', 0)}`",
                f"- Severity: `{record.get('severity', '')}`",
                f"- Retryable: `{_bool_text(record.get('retryable'))}`",
                f"- Requires manual review: "
                f"`{_bool_text(record.get('requires_manual_review'))}`",
            ]
        )
        items = _items(record.get("items"))
        if not items:
            lines.append("- Items: None.")
        else:
            lines.append("- Items:")
            lines.extend(f"  - {item.get('reason', 'unspecified')}" for item in items)
        lines.append("")
    lines.extend(
        [
            "## Retry Readiness",
            "",
            f"- Safe to retry: `{_bool_text(retry.get('safe_to_retry'))}`",
            f"- Retry mode: `{retry.get('retry_mode', 'UNKNOWN')}`",
            "- Retry candidates:",
            *_markdown_list(_strings(retry.get("retry_candidates"))),
            "- Retry blockers:",
            *_markdown_list(_strings(retry.get("retry_blockers"))),
            "",
            "## Recommended Actions",
            "",
            *_markdown_list(_strings(payload.get("recommended_actions"))),
            "",
            "## Safety Invariants",
            "",
            f"- Read only: `{_bool_text(safety.get('read_only'))}`",
            f"- No external delivery: `{_bool_text(safety.get('no_external_delivery'))}`",
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


def render_notification_delivery_failure_classification_log(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    source = _mapping(payload.get("source_audit"))
    summary = _mapping(payload.get("classification_summary"))
    outputs = _mapping(payload.get("output_artifacts"))
    return "\n".join(
        [
            f"task_id={metadata.get('task_id', TASK_ID)}",
            f"generated_at={metadata.get('generated_at', '')}",
            f"source_audit_status={source.get('audit_status', STATUS_UNKNOWN)}",
            f"source_parse_status={source.get('source_parse_status', STATUS_UNKNOWN)}",
            f"overall_status={summary.get('overall_status', STATUS_UNKNOWN)}",
            f"highest_severity={summary.get('highest_severity', SEVERITY_NONE)}",
            f"requires_manual_review={_bool_text(summary.get('requires_manual_review'))}",
            f"safe_to_retry={_bool_text(summary.get('safe_to_retry'))}",
            "production_effect=none",
            "manual_review_only=true",
            "read_only=true",
            "no_external_delivery=true",
            "no_state_mutation=true",
            "no_production_parameter_change=true",
            f"classification_json={_mapping(outputs.get('classification_json')).get('path', '')}",
            f"classification_markdown="
            f"{_mapping(outputs.get('classification_markdown')).get('path', '')}",
            f"run_log={_mapping(outputs.get('run_log')).get('path', '')}",
            "",
        ]
    )


def _resolve_source_path(
    *,
    project_root: Path,
    as_of: date | None,
    audit_summary_path: Path | None,
    generated_at: datetime,
) -> Path:
    if audit_summary_path is not None:
        return _resolve_project_path(project_root, audit_summary_path)
    latest = find_latest_notification_delivery_audit_summary(
        project_root=project_root,
        as_of=as_of,
    )
    if latest is not None:
        return latest
    fallback_date = as_of or generated_at.date()
    return (
        project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "delivery_audit"
        / f"notification_delivery_audit_summary_{fallback_date.isoformat()}.json"
    )


def _read_source_json(path: Path) -> tuple[dict[str, Any], bool, str, str]:
    if not path.exists():
        return {}, False, "MISSING", f"Source audit artifact not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, True, "MALFORMED_JSON", f"Source audit JSON malformed: {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, True, "INVALID_SHAPE", f"Source audit JSON must be an object: {path}"
    return payload, True, "OK", ""


def _classification_date(
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
    path_date = _audit_date_from_path(source_path)
    if path_date is not None:
        return path_date
    return generated_at.date()


def _source_audit_status(source_payload: dict[str, Any], source_parse_status: str) -> str:
    if source_parse_status == "MISSING":
        return "MISSING"
    if source_parse_status != "OK":
        return STATUS_UNKNOWN
    return _string_value(source_payload.get("audit_status")) or STATUS_UNKNOWN


def _source_generated_at(source_payload: dict[str, Any]) -> str:
    audit = _mapping(source_payload.get("audit"))
    return (
        _string_value(source_payload.get("generated_at"))
        or _string_value(source_payload.get("created_at"))
        or _string_value(audit.get("created_at"))
    )


def _source_reasons(
    *,
    source_payload: dict[str, Any],
    source_available: bool,
    source_parse_status: str,
    source_error: str,
) -> list[str]:
    if not source_available:
        return [source_error]
    if source_parse_status != "OK":
        return [source_error]
    reasons: list[str] = []
    alerts = _mapping(source_payload.get("alerts"))
    reasons.extend(_strings(alerts.get("critical")))
    reasons.extend(_strings(alerts.get("warnings")))
    chain = _mapping(source_payload.get("artifact_chain"))
    reasons.extend(_strings(chain.get("blocking_reasons")))
    reasons.extend(_strings(chain.get("warnings")))
    safety = _mapping(source_payload.get("safety_validation"))
    reasons.extend(_strings(safety.get("blocking_reasons")))
    side_effects = _mapping(source_payload.get("external_side_effect_audit"))
    reasons.extend(_strings(side_effects.get("blocking_reasons")))
    artifacts = _mapping(source_payload.get("input_artifacts"))
    for name, record in artifacts.items():
        artifact = _mapping(record)
        status = _string_value(artifact.get("status"))
        error = _string_value(artifact.get("error"))
        if status and status not in {"FOUND", "PASS"}:
            reasons.append(f"{name}: {status}")
        if error:
            reasons.append(error)
    return _unique_strings(reasons) or [
        f"TRADING-035 audit_status={source_payload.get('audit_status')}"
    ]


def _empty_failure_categories() -> dict[str, dict[str, Any]]:
    return {
        name: {
            "count": 0,
            "severity": severity,
            "retryable": retryable,
            "requires_manual_review": requires_review,
            "items": [],
        }
        for name, severity, retryable, requires_review in CATEGORY_DEFINITIONS
    }


def _classify_into_categories(
    *,
    categories: dict[str, dict[str, Any]],
    audit_status: str,
    source_available: bool,
    source_parse_status: str,
    source_path: Path,
    reasons: list[str],
) -> None:
    if audit_status == AUDIT_PASS and source_parse_status == "OK":
        return
    if not source_available:
        _add_category_item(
            categories,
            CATEGORY_MISSING_ARTIFACT,
            audit_status=audit_status,
            source_path=source_path,
            reasons=reasons,
        )
        return
    if source_parse_status != "OK":
        _add_category_item(
            categories,
            CATEGORY_UNKNOWN,
            audit_status=audit_status,
            source_path=source_path,
            reasons=reasons,
        )
        return
    if audit_status == AUDIT_INCOMPLETE:
        category = (
            CATEGORY_CONFIGURATION
            if _contains_token(reasons, CONFIGURATION_REASON_TOKENS)
            else CATEGORY_MISSING_ARTIFACT
        )
        _add_category_item(
            categories,
            category,
            audit_status=audit_status,
            source_path=source_path,
            reasons=reasons,
        )
        return
    if audit_status == AUDIT_MISMATCH:
        _add_category_item(
            categories,
            CATEGORY_CONTENT_MISMATCH,
            audit_status=audit_status,
            source_path=source_path,
            reasons=reasons,
        )
        return
    if audit_status == AUDIT_SAFETY_BLOCKED:
        _add_category_item(
            categories,
            CATEGORY_SAFETY_BLOCKED,
            audit_status=audit_status,
            source_path=source_path,
            reasons=reasons,
        )
        return
    if audit_status == AUDIT_PASS_WITH_WARNINGS:
        category = (
            CATEGORY_TRANSIENT
            if _contains_token(reasons, TRANSIENT_REASON_TOKENS)
            else CATEGORY_UNKNOWN
        )
        _add_category_item(
            categories,
            category,
            audit_status=audit_status,
            source_path=source_path,
            reasons=reasons,
        )
        return
    _add_category_item(
        categories,
        CATEGORY_UNKNOWN,
        audit_status=audit_status,
        source_path=source_path,
        reasons=reasons,
    )


def _add_category_item(
    categories: dict[str, dict[str, Any]],
    category: str,
    *,
    audit_status: str,
    source_path: Path,
    reasons: list[str],
) -> None:
    record = categories[category]
    items = _items(record.get("items"))
    reason = reasons[0] if reasons else f"TRADING-035 audit_status={audit_status}"
    items.append(
        {
            "source_audit_status": audit_status,
            "source_audit_path": str(source_path),
            "reason": reason,
            "details": reasons,
        }
    )
    record["items"] = items
    record["count"] = len(items)


def _classification_summary(
    categories: dict[str, dict[str, Any]],
    audit_status: str,
) -> dict[str, Any]:
    counts = {name: _int_value(record.get("count")) for name, record in categories.items()}
    total_findings = sum(counts.values())
    total_failures = sum(
        count
        for name, count in counts.items()
        if name != CATEGORY_TRANSIENT or audit_status != AUDIT_PASS_WITH_WARNINGS
    )
    if total_findings == 0 and audit_status == AUDIT_PASS:
        return {
            "overall_status": STATUS_PASS,
            "total_findings": 0,
            "total_failures": 0,
            "highest_severity": SEVERITY_NONE,
            "requires_manual_review": False,
            "safe_to_retry": False,
            "blocks_notification_chain": False,
        }
    highest = _highest_severity(categories)
    overall = _overall_status(highest, audit_status)
    requires_manual_review = any(
        _int_value(record.get("count")) > 0 and record.get("requires_manual_review") is True
        for record in categories.values()
    )
    safe_to_retry = (
        total_findings > 0
        and counts.get(CATEGORY_TRANSIENT, 0) == total_findings
        and not requires_manual_review
    )
    return {
        "overall_status": overall,
        "total_findings": total_findings,
        "total_failures": total_failures,
        "highest_severity": highest,
        "requires_manual_review": requires_manual_review,
        "safe_to_retry": safe_to_retry,
        "blocks_notification_chain": total_findings > 0,
    }


def _retry_readiness(
    categories: dict[str, dict[str, Any]],
    summary: dict[str, Any],
    reasons: list[str],
) -> dict[str, Any]:
    safe_to_retry = summary.get("safe_to_retry") is True
    retry_candidates: list[str] = []
    manual_review_required_for: list[str] = []
    retry_blockers = list(reasons)
    for category, record in categories.items():
        if _int_value(record.get("count")) <= 0:
            continue
        if record.get("retryable") is True and record.get("requires_manual_review") is not True:
            retry_candidates.append(category)
        if record.get("requires_manual_review") is True:
            manual_review_required_for.append(category)
    if summary.get("overall_status") == STATUS_PASS:
        retry_mode = "NOT_APPLICABLE"
        retry_blockers = []
    elif summary.get("overall_status") == STATUS_CRITICAL:
        retry_mode = "SAFETY_BLOCKED"
    elif safe_to_retry:
        retry_mode = "READY_FOR_RETRY"
        retry_blockers = []
    else:
        retry_mode = "BLOCKED_PENDING_MANUAL_REVIEW"
    return {
        "safe_to_retry": safe_to_retry,
        "retry_mode": retry_mode,
        "retry_candidates": retry_candidates,
        "retry_blockers": _unique_strings(retry_blockers),
        "manual_review_required_for": _unique_strings(manual_review_required_for),
    }


def _recommended_actions(
    *,
    overall_status: str,
    audit_status: str,
    categories: dict[str, dict[str, Any]],
) -> list[str]:
    if overall_status == STATUS_PASS:
        return ["No notification delivery failures detected; no retry is required."]
    if _int_value(categories[CATEGORY_SAFETY_BLOCKED].get("count")) > 0:
        return [
            "SAFETY BLOCKED: manual review required before any retry or delivery action.",
            "Stop notification handling until the safety anomaly is resolved.",
            "Do not send, retry, or deliver from this artifact chain.",
        ]
    if _int_value(categories[CATEGORY_CONTENT_MISMATCH].get("count")) > 0:
        return [
            "Review the TRADING-035 artifact chain mismatch before any retry.",
            "Regenerate or re-approve upstream artifacts only through their dedicated tasks.",
            "Do not use the notification chain for delivery until the mismatch is explained.",
        ]
    if _int_value(categories[CATEGORY_CONFIGURATION].get("count")) > 0:
        return [
            "Review notification target, recipient, and channel configuration manually.",
            "Do not retry until configuration ownership and intended target are confirmed.",
        ]
    if _int_value(categories[CATEGORY_MISSING_ARTIFACT].get("count")) > 0:
        return [
            "Restore or regenerate the missing source artifact through the owning task.",
            "Re-run TRADING-035 before re-running this classifier.",
            "Do not infer notification readiness from partial artifacts.",
        ]
    if audit_status == AUDIT_PASS_WITH_WARNINGS:
        return [
            "Review the TRADING-035 warning before treating the notification chain as complete.",
            "No automatic retry is permitted from TRADING-036.",
        ]
    return [
        "Review the source TRADING-035 audit summary and classifier output manually.",
        "Do not retry or deliver until the unexpected status is classified by a future rule.",
    ]


def _highest_severity(categories: dict[str, dict[str, Any]]) -> str:
    winner = SEVERITY_NONE
    for record in categories.values():
        if _int_value(record.get("count")) <= 0:
            continue
        severity = _string_value(record.get("severity"))
        if _severity_rank(severity) > _severity_rank(winner):
            winner = severity
    return winner


def _overall_status(highest: str, audit_status: str) -> str:
    if highest == SEVERITY_CRITICAL:
        return STATUS_CRITICAL
    if highest == SEVERITY_ERROR:
        return STATUS_ERROR
    if audit_status == AUDIT_PASS_WITH_WARNINGS:
        return STATUS_WARN
    if highest == SEVERITY_WARN:
        return STATUS_UNKNOWN
    return STATUS_UNKNOWN


def _status_banner(overall_status: str) -> str:
    if overall_status == STATUS_PASS:
        return "PASS: No notification delivery failures detected."
    if overall_status == STATUS_WARN:
        return "WARN: Retry may be possible only after review."
    if overall_status == STATUS_ERROR:
        return "ERROR: Manual review required."
    if overall_status == STATUS_CRITICAL:
        return "CRITICAL: Safety blocked. Do not retry or deliver."
    return "UNKNOWN: Manual review required before retry or delivery."


def _severity_rank(severity: str) -> int:
    return {
        SEVERITY_NONE: 0,
        SEVERITY_WARN: 1,
        SEVERITY_ERROR: 2,
        SEVERITY_CRITICAL: 3,
    }.get(severity, 0)


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-036 production_effect must be none")
    true_fields = (
        "manual_review_only",
        "notification_delivery_failure_classification_only",
        "read_only",
    )
    for field in true_fields:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-036 {field} must be true")
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
        "production_parameters_modified",
        "notification_delivery_audit_executed_by_classifier",
        "notification_draft_executed_by_classifier",
        "delivery_preflight_executed_by_classifier",
        "draft_dispatch_executed_by_classifier",
        "operator_brief_executed_by_classifier",
        "pipelines_executed_by_classifier",
        "data_downloaded_by_classifier",
        "apply_executed_by_classifier",
        "rollback_executed_by_classifier",
        "broker_execution",
        "replay_execution",
        "trading_execution",
    )
    for field in false_fields:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-036 {field} must be false")
    safety = _mapping(payload.get("safety_invariants"))
    for field in (
        "read_only",
        "no_external_delivery",
        "no_state_mutation",
        "no_production_parameter_change",
        "dashboard_read_only",
    ):
        if safety.get(field) is not True:
            raise ValueError(f"TRADING-036 safety_invariants.{field} must be true")


def _audit_date_from_path(path: Path) -> date | None:
    raw_date = path.stem.removeprefix("notification_delivery_audit_summary_")
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


def _contains_token(values: list[str], tokens: tuple[str, ...]) -> bool:
    joined = "\n".join(values).casefold()
    return any(token.casefold() in joined for token in tokens)


def _markdown_list(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _bool_text(value: object) -> str:
    return "true" if value is True else "false"


def _isoformat(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")
