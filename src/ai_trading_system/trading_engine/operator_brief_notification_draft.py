from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "operator_brief_notification_draft"
RUN_REPORT_TYPE = "operator_brief_notification_draft_run"
TASK_ID = "TRADING-030"
INPUT_TASK_ID = "TRADING-022"
MODE = "operator_brief_notification_draft_only"
PRODUCTION_EFFECT_NONE = "none"

DRAFT_GENERATED = "GENERATED"
DRAFT_GENERATED_WITH_WARNINGS = "GENERATED_WITH_WARNINGS"
DRAFT_INPUT_MISSING = "INPUT_MISSING"
DRAFT_INPUT_INVALID = "INPUT_INVALID"
DRAFT_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DRAFT_ERROR = "ERROR"

SEVERITY_NORMAL = "NORMAL"
SEVERITY_WATCH = "WATCH"
SEVERITY_ACTION = "ACTION"
SEVERITY_URGENT = "URGENT"
SEVERITY_BLOCKED = "BLOCKED"
SEVERITY_UNKNOWN = "UNKNOWN"

STATUS_FOUND = "FOUND"
STATUS_MISSING = "MISSING"
STATUS_INVALID = "INVALID"
STATUS_OPTIONAL_FOUND = "OPTIONAL_FOUND"
STATUS_OPTIONAL_MISSING = "OPTIONAL_MISSING"
STATUS_OPTIONAL_INVALID = "OPTIONAL_INVALID"

BRIEF_STATUS_TO_SEVERITY = {
    "OK": SEVERITY_NORMAL,
    "WATCH": SEVERITY_WATCH,
    "ACTION_REQUIRED": SEVERITY_ACTION,
    "URGENT": SEVERITY_URGENT,
    "SAFETY_BLOCKED": SEVERITY_BLOCKED,
    "INPUT_MISSING": SEVERITY_UNKNOWN,
    "INPUT_INVALID": SEVERITY_UNKNOWN,
    "ERROR": SEVERITY_UNKNOWN,
}

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

SAFETY_TRUE_FIELDS = (
    "manual_review_only",
    "notification_draft_only",
    "read_only",
    "safe_for_scheduler",
)
SAFETY_FALSE_FIELDS = (
    "email_sent",
    "gmail_draft_created",
    "slack_sent",
    "discord_sent",
    "mobile_push_sent",
    "operator_brief_executed_by_notification_draft",
    "pipelines_executed_by_notification_draft",
    "data_downloaded_by_notification_draft",
    "apply_executed_by_notification_draft",
    "rollback_executed_by_notification_draft",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)
OPERATOR_BRIEF_FALSE_FIELDS = (
    "apply_executed_by_operator_brief",
    "rollback_executed_by_operator_brief",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)

SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"\b(api_key|secret|token|password|credential|account_id)\b\s*([:=])\s*([^\s,;`]+)",
    re.IGNORECASE,
)


def default_notification_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs" / "notifications"


def default_notification_metadata_path(data_root: Path, as_of: date) -> Path:
    return default_notification_root(data_root) / (
        f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    )


def default_notification_markdown_path(data_root: Path, as_of: date) -> Path:
    return default_notification_metadata_path(data_root, as_of).with_suffix(".md")


def default_email_draft_path(data_root: Path, as_of: date) -> Path:
    return (
        default_notification_root(data_root)
        / "email"
        / f"operator_brief_email_draft_{as_of.isoformat()}.md"
    )


def default_chat_draft_path(data_root: Path, as_of: date) -> Path:
    return (
        default_notification_root(data_root)
        / "chat"
        / f"operator_brief_chat_draft_{as_of.isoformat()}.md"
    )


def default_mobile_summary_path(data_root: Path, as_of: date) -> Path:
    return (
        default_notification_root(data_root)
        / "mobile"
        / f"operator_brief_mobile_summary_{as_of.isoformat()}.md"
    )


def default_notification_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_notification_root(data_root)
        / "logs"
        / f"operator_brief_notification_draft_run_{as_of.isoformat()}.json"
    )


def default_operator_brief_json_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "operator_briefs"
        / (f"daily_trading_system_operator_brief_{as_of.isoformat()}.json")
    )


def write_operator_brief_notification_draft(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    operator_brief_file: Path | None = None,
    parameter_governance_digest_file: Path | None = None,
    pipeline_health_summary_file: Path | None = None,
    data_freshness_summary_file: Path | None = None,
    scheduler_dry_run_file: Path | None = None,
    audience: str = "personal",
    max_lines: int = 20,
    include_links: bool = False,
    fail_on_urgent: bool = False,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = Path(data_root)
    metadata_path = default_notification_metadata_path(data_root, as_of)
    summary_markdown_path = default_notification_markdown_path(data_root, as_of)
    email_path = default_email_draft_path(data_root, as_of)
    chat_path = default_chat_draft_path(data_root, as_of)
    mobile_path = default_mobile_summary_path(data_root, as_of)
    run_log_json_path = default_notification_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    try:
        bundle = build_operator_brief_notification_draft(
            as_of=as_of,
            data_root=data_root,
            operator_brief_file=operator_brief_file,
            parameter_governance_digest_file=parameter_governance_digest_file,
            pipeline_health_summary_file=pipeline_health_summary_file,
            data_freshness_summary_file=data_freshness_summary_file,
            scheduler_dry_run_file=scheduler_dry_run_file,
            audience=audience,
            max_lines=max_lines,
            include_links=include_links,
            metadata_path=metadata_path,
            summary_markdown_path=summary_markdown_path,
            email_path=email_path,
            chat_path=chat_path,
            mobile_path=mobile_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
        )
    except Exception as exc:  # pragma: no cover - defensive artifact path
        bundle = _error_bundle(
            as_of=as_of,
            data_root=data_root,
            metadata_path=metadata_path,
            summary_markdown_path=summary_markdown_path,
            email_path=email_path,
            chat_path=chat_path,
            mobile_path=mobile_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    payload = bundle["payload"]
    _write_json(metadata_path, payload)
    _write_text(summary_markdown_path, bundle["summary_markdown"])
    _write_text(email_path, bundle["email_draft"])
    _write_text(chat_path, bundle["chat_draft"])
    _write_text(mobile_path, bundle["mobile_summary"])
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_operator_brief_notification_draft_run_log(run_log))

    if fail_on_urgent and payload.get("notification_severity") == SEVERITY_URGENT:
        raise SystemExit(2)
    return payload


def build_operator_brief_notification_draft(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    operator_brief_file: Path | None = None,
    parameter_governance_digest_file: Path | None = None,
    pipeline_health_summary_file: Path | None = None,
    data_freshness_summary_file: Path | None = None,
    scheduler_dry_run_file: Path | None = None,
    audience: str = "personal",
    max_lines: int = 20,
    include_links: bool = False,
    metadata_path: Path | None = None,
    summary_markdown_path: Path | None = None,
    email_path: Path | None = None,
    chat_path: Path | None = None,
    mobile_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = Path(data_root)
    metadata_path = metadata_path or default_notification_metadata_path(data_root, as_of)
    summary_markdown_path = summary_markdown_path or metadata_path.with_suffix(".md")
    email_path = email_path or default_email_draft_path(data_root, as_of)
    chat_path = chat_path or default_chat_draft_path(data_root, as_of)
    mobile_path = mobile_path or default_mobile_summary_path(data_root, as_of)
    run_log_json_path = run_log_json_path or default_notification_run_log_json_path(
        data_root, as_of
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")

    operator_path = _resolve_operator_brief_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=operator_brief_file,
    )
    operator_payload, operator_status, operator_error = _read_json_object_with_status(operator_path)
    optional_inputs = _load_optional_inputs(
        as_of=as_of,
        data_root=data_root,
        parameter_governance_digest_file=parameter_governance_digest_file,
        pipeline_health_summary_file=pipeline_health_summary_file,
        data_freshness_summary_file=data_freshness_summary_file,
        scheduler_dry_run_file=scheduler_dry_run_file,
    )

    redaction_warnings: list[str] = []
    warning_keys: set[str] = set()
    input_warnings: list[str] = []
    safety = _operator_brief_safety_validation(
        operator_payload,
        operator_status=operator_status,
        operator_error=operator_error,
    )
    draft_status = _draft_status(
        operator_status=operator_status,
        operator_payload=operator_payload,
        safety=safety,
    )
    brief_status = _brief_status_for_payload(operator_payload, draft_status)
    notification_severity = (
        SEVERITY_BLOCKED
        if draft_status == DRAFT_SAFETY_BLOCKED
        else BRIEF_STATUS_TO_SEVERITY.get(brief_status, SEVERITY_UNKNOWN)
    )
    source_snapshot = _source_snapshot(
        operator_payload=operator_payload,
        draft_status=draft_status,
        notification_severity=notification_severity,
    )
    source_snapshot["headline"] = _redact_text(
        str(source_snapshot.get("headline") or ""),
        redaction_warnings,
        warning_keys,
        context="operator brief headline",
    )
    _record_sensitive_source_texts(
        operator_payload,
        redaction_warnings=redaction_warnings,
        warning_keys=warning_keys,
    )
    headline = str(source_snapshot.get("headline") or "")
    input_artifacts = _input_artifacts(operator_path, operator_status, optional_inputs)
    input_warnings.extend(_optional_input_warnings(optional_inputs))
    if source_snapshot["warning_count"]:
        input_warnings.append("Operator brief contains warning alerts.")

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_draft_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "slack_sent": False,
        "discord_sent": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_notification_draft": False,
        "pipelines_executed_by_notification_draft": False,
        "data_downloaded_by_notification_draft": False,
        "apply_executed_by_notification_draft": False,
        "rollback_executed_by_notification_draft": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "draft_status": draft_status,
        "notification_severity": notification_severity,
        "headline": headline,
        "audience": audience,
        "input_artifacts": input_artifacts,
        "source_snapshot": source_snapshot,
        "draft_outputs": {
            "email_draft": {
                "path": str(email_path),
                "subject": _email_subject(brief_status, as_of),
            },
            "chat_draft": {"path": str(chat_path)},
            "mobile_summary": {"path": str(mobile_path)},
            "summary_markdown": {"path": str(summary_markdown_path)},
        },
        "notification_content_summary": {
            "email_lines": 0,
            "chat_lines": 0,
            "mobile_lines": 0,
            "contains_urgent_banner": False,
            "contains_action_required_banner": False,
            "contains_safety_blocked_banner": False,
        },
        "safety_validation": safety,
        "manual_review_required": {
            "required": True,
            "instructions": [
                "Review notification drafts before sending.",
                "Do not send urgent notifications without checking the underlying operator brief.",
                "Do not attach credentials or account information.",
            ],
        },
        "alerts": {
            "critical": [],
            "warnings": input_warnings,
            "notes": ["TRADING-030 generated notification drafts only."],
        },
        "audit": {
            "created_by": "scripts/generate_operator_brief_notification_draft.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_notification_drafts": True,
        },
        "output_artifacts": {
            "metadata_json": {"path": str(metadata_path)},
            "summary_markdown": {"path": str(summary_markdown_path)},
            "email_draft": {"path": str(email_path)},
            "chat_draft": {"path": str(chat_path)},
            "mobile_summary": {"path": str(mobile_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
    }
    if input_warnings and payload["draft_status"] == DRAFT_GENERATED:
        payload["draft_status"] = DRAFT_GENERATED_WITH_WARNINGS

    drafts = _render_drafts(
        payload=payload,
        operator_payload=operator_payload,
        max_lines=max_lines,
        include_links=include_links,
        redaction_warnings=redaction_warnings,
        warning_keys=warning_keys,
    )
    if redaction_warnings and payload["draft_status"] == DRAFT_GENERATED:
        payload["draft_status"] = DRAFT_GENERATED_WITH_WARNINGS
    if redaction_warnings:
        payload["alerts"]["warnings"] = _unique_strings(
            [*payload["alerts"]["warnings"], *redaction_warnings]
        )
    drafts = _render_drafts(
        payload=payload,
        operator_payload=operator_payload,
        max_lines=max_lines,
        include_links=include_links,
        redaction_warnings=redaction_warnings,
        warning_keys=warning_keys,
    )
    payload["notification_content_summary"] = _content_summary(drafts)
    _assert_notification_safety_invariants(payload)
    return {"payload": payload, **drafts}


def render_operator_brief_notification_summary_markdown(payload: dict[str, Any]) -> str:
    snapshot = _mapping(payload.get("source_snapshot"))
    outputs = _mapping(payload.get("draft_outputs"))
    email = _mapping(outputs.get("email_draft"))
    chat = _mapping(outputs.get("chat_draft"))
    mobile = _mapping(outputs.get("mobile_summary"))
    return "\n".join(
        [
            f"# Operator Brief Notification Draft - {payload.get('date')}",
            "",
            "## 1. Draft Summary",
            "",
            f"- Draft Status: `{payload.get('draft_status')}`",
            f"- Notification Severity: `{payload.get('notification_severity')}`",
            f"- Email Sent: `{_bool_text(payload.get('email_sent'))}`",
            f"- Slack Sent: `{_bool_text(payload.get('slack_sent'))}`",
            f"- Discord Sent: `{_bool_text(payload.get('discord_sent'))}`",
            f"- Mobile Push Sent: `{_bool_text(payload.get('mobile_push_sent'))}`",
            "- Manual Review Required: "
            f"`{_bool_text(_mapping(payload.get('manual_review_required')).get('required'))}`",
            "",
            "## 2. Source Brief Snapshot",
            "",
            f"- Brief Status: `{snapshot.get('brief_status')}`",
            f"- Summary Level: `{snapshot.get('summary_level')}`",
            f"- Can Trust Outputs Today: `{_bool_text(snapshot.get('can_trust_outputs_today'))}`",
            f"- Manual Action Required: `{_bool_text(snapshot.get('manual_action_required'))}`",
            f"- Headline: {payload.get('headline') or ''}",
            "",
            "## 3. Draft Outputs",
            "",
            "| Draft | Path |",
            "|---|---|",
            f"| Email | `{email.get('path', '')}` |",
            f"| Chat | `{chat.get('path', '')}` |",
            f"| Mobile | `{mobile.get('path', '')}` |",
            "",
            "## 4. Safety Statement",
            "",
            "TRADING-030 only generates notification drafts.",
            "",
            "It does not:",
            "- send email",
            "- create Gmail draft",
            "- call Slack / Discord webhook",
            "- send mobile notification",
            "- run operator brief",
            "- run apply / rollback",
            "- run broker / replay / trading",
            "",
            "## 5. Manual Review Checklist",
            "",
            "- [ ] Review operator brief source.",
            "- [ ] Review notification draft content.",
            "- [ ] Confirm no credentials or account info are included.",
            "- [ ] Send manually only if appropriate.",
            "",
        ]
    )


def render_operator_brief_notification_draft_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Operator Brief Notification Draft Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- draft_status: `{payload.get('draft_status')}`",
            f"- notification_severity: `{payload.get('notification_severity')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- notification_draft_only: `true`",
            "- read_only: `true`",
            "- email_sent: `false`",
            "- gmail_draft_created: `false`",
            "- slack_sent: `false`",
            "- discord_sent: `false`",
            "- mobile_push_sent: `false`",
            "- operator_brief_executed_by_notification_draft: `false`",
            "- pipelines_executed_by_notification_draft: `false`",
            "- data_downloaded_by_notification_draft: `false`",
            "- apply_executed_by_notification_draft: `false`",
            "- rollback_executed_by_notification_draft: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- notification_metadata_json: `{payload.get('notification_metadata_json')}`",
            f"- notification_summary_markdown: `{payload.get('notification_summary_markdown')}`",
            "",
        ]
    )


def should_fail_cli(payload: dict[str, Any], *, fail_on_urgent: bool = False) -> bool:
    return fail_on_urgent and payload.get("notification_severity") == SEVERITY_URGENT


def _render_drafts(
    *,
    payload: dict[str, Any],
    operator_payload: dict[str, Any],
    max_lines: int,
    include_links: bool,
    redaction_warnings: list[str],
    warning_keys: set[str],
) -> dict[str, str]:
    email = _render_email_draft(
        payload=payload,
        operator_payload=operator_payload,
        max_lines=max_lines,
        include_links=include_links,
        redaction_warnings=redaction_warnings,
        warning_keys=warning_keys,
    )
    chat = _render_chat_draft(
        payload=payload,
        operator_payload=operator_payload,
        max_lines=max_lines,
        redaction_warnings=redaction_warnings,
        warning_keys=warning_keys,
    )
    mobile = _render_mobile_summary(payload=payload)
    summary = render_operator_brief_notification_summary_markdown(payload)
    return {
        "email_draft": email,
        "chat_draft": chat,
        "mobile_summary": mobile,
        "summary_markdown": summary,
    }


def _render_email_draft(
    *,
    payload: dict[str, Any],
    operator_payload: dict[str, Any],
    max_lines: int,
    include_links: bool,
    redaction_warnings: list[str],
    warning_keys: set[str],
) -> str:
    snapshot = _mapping(payload.get("source_snapshot"))
    severity = str(payload.get("notification_severity") or SEVERITY_UNKNOWN)
    brief_status = str(snapshot.get("brief_status") or "UNKNOWN")
    subject = _mapping(_mapping(payload.get("draft_outputs")).get("email_draft")).get("subject")
    governance = str(snapshot.get("parameter_governance_status") or "UNKNOWN")
    pipeline = str(snapshot.get("pipeline_health_status") or "UNKNOWN")
    freshness = str(snapshot.get("data_freshness_status") or "UNKNOWN")
    recommended = _redacted_strings(
        _strings(operator_payload.get("recommended_next_steps")),
        redaction_warnings,
        warning_keys,
        context="operator brief recommended next steps",
    )[: max(1, min(max_lines, 5))]
    if not recommended:
        recommended = _recommended_next_steps(severity)
    lines = [f"Subject: {subject}", "", "# Daily Trading System Operator Brief", ""]
    if severity == SEVERITY_URGENT:
        lines.extend(["## URGENT: Manual Attention Required", ""])
    elif severity == SEVERITY_ACTION:
        lines.extend(["## Action Required", ""])
    elif severity == SEVERITY_BLOCKED:
        lines.extend(["## Notification Draft Safety Blocked", ""])

    lines.extend(
        [
            "## Status",
            "",
            f"- Brief Status: {brief_status}",
            f"- Summary Level: {snapshot.get('summary_level') or 'UNKNOWN'}",
            "- Can Trust Outputs Today: " f"{_bool_text(snapshot.get('can_trust_outputs_today'))}",
            "- Manual Action Required: " f"{_bool_text(snapshot.get('manual_action_required'))}",
            "",
            "## Headline",
            "",
            str(payload.get("headline") or ""),
            "",
            "## Key Modules",
            "",
            f"- Parameter Governance: {governance}",
            f"- Pipeline Health: {pipeline}",
            f"- Data Freshness: {freshness}",
            "",
            "## Alerts",
            "",
            f"Critical: {snapshot.get('critical_alert_count') or 0}",
            f"Warnings: {snapshot.get('warning_count') or 0}",
            "",
            "## Recommended Next Steps",
            "",
        ]
    )
    lines.extend([f"- {item}" for item in recommended])
    if include_links:
        lines.extend(["", "## Links", ""])
        for label, artifact in _mapping(payload.get("input_artifacts")).items():
            path = _mapping(artifact).get("path") or ""
            lines.append(f"- {_artifact_label(label)}: `{path}`")
    lines.extend(["", "---", "", "This is a draft only. No email was sent.", ""])
    return "\n".join(lines)


def _render_chat_draft(
    *,
    payload: dict[str, Any],
    operator_payload: dict[str, Any],
    max_lines: int,
    redaction_warnings: list[str],
    warning_keys: set[str],
) -> str:
    snapshot = _mapping(payload.get("source_snapshot"))
    severity = str(payload.get("notification_severity") or SEVERITY_UNKNOWN)
    recommended = _redacted_strings(
        _strings(operator_payload.get("recommended_next_steps")),
        redaction_warnings,
        warning_keys,
        context="operator brief recommended next steps",
    )[: max(1, min(max_lines, 3))]
    if not recommended:
        recommended = _recommended_next_steps(severity)[:1]
    lines: list[str] = []
    if severity == SEVERITY_URGENT:
        lines.extend([":rotating_light: **URGENT: Trading System Manual Attention Required**", ""])
    elif severity == SEVERITY_ACTION:
        lines.extend(["**Action Required: Trading System Operator Brief**", ""])
    elif severity == SEVERITY_BLOCKED:
        lines.extend(["**Notification Draft Safety Blocked**", ""])
    lines.extend(
        [
            f"**Daily Trading System Operator Brief - {payload.get('date')}**",
            "",
            f"Status: {snapshot.get('brief_status') or 'UNKNOWN'}",
            f"Action Required: {_bool_text(snapshot.get('manual_action_required'))}",
            f"Trust Outputs Today: {_bool_text(snapshot.get('can_trust_outputs_today'))}",
            "",
            f"Parameter Governance: {snapshot.get('parameter_governance_status') or 'UNKNOWN'}",
            f"Pipeline Health: {snapshot.get('pipeline_health_status') or 'UNKNOWN'}",
            f"Data Freshness: {snapshot.get('data_freshness_status') or 'UNKNOWN'}",
            "",
            "Headline:",
            str(payload.get("headline") or ""),
            "",
            "Next:",
        ]
    )
    lines.extend([f"- {item}" for item in recommended])
    lines.extend(["", "_Draft only. No Slack or Discord webhook was called._", ""])
    return "\n".join(lines)


def _render_mobile_summary(*, payload: dict[str, Any]) -> str:
    severity = str(payload.get("notification_severity") or SEVERITY_UNKNOWN)
    if severity == SEVERITY_NORMAL:
        return "Trading System OK - no manual action required.\n"
    if severity == SEVERITY_WATCH:
        return "Trading System WATCH - monitoring recommended.\n"
    if severity == SEVERITY_ACTION:
        return "Trading System ACTION REQUIRED - review pending items.\n"
    if severity == SEVERITY_URGENT:
        return "URGENT: Trading System issue detected - inspect operator brief now.\n"
    if severity == SEVERITY_BLOCKED:
        return "BLOCKED: Notification draft safety check failed.\n"
    return "Trading System status UNKNOWN - review operator brief input.\n"


def _content_summary(drafts: dict[str, str]) -> dict[str, Any]:
    email = drafts["email_draft"]
    chat = drafts["chat_draft"]
    mobile = drafts["mobile_summary"]
    return {
        "email_lines": _line_count(email),
        "chat_lines": _line_count(chat),
        "mobile_lines": _line_count(mobile),
        "contains_urgent_banner": "URGENT: Manual Attention Required" in email
        or "URGENT: Trading System Manual Attention Required" in chat,
        "contains_action_required_banner": "## Action Required" in email
        or "Action Required: Trading System Operator Brief" in chat,
        "contains_safety_blocked_banner": "Notification Draft Safety Blocked" in email
        or "Notification Draft Safety Blocked" in chat,
    }


def _draft_status(
    *,
    operator_status: str,
    operator_payload: dict[str, Any],
    safety: dict[str, Any],
) -> str:
    if operator_status == STATUS_MISSING:
        return DRAFT_INPUT_MISSING
    if operator_status == STATUS_INVALID:
        return DRAFT_INPUT_INVALID
    if operator_payload.get("task_id") != INPUT_TASK_ID:
        return DRAFT_INPUT_INVALID
    if safety.get("status") != "PASS":
        return DRAFT_SAFETY_BLOCKED
    return DRAFT_GENERATED


def _brief_status_for_payload(operator_payload: dict[str, Any], draft_status: str) -> str:
    if draft_status == DRAFT_INPUT_MISSING:
        return DRAFT_INPUT_MISSING
    if draft_status == DRAFT_INPUT_INVALID:
        return DRAFT_INPUT_INVALID
    if draft_status == DRAFT_ERROR:
        return DRAFT_ERROR
    return _string_value(operator_payload.get("brief_status")) or "ERROR"


def _source_snapshot(
    *,
    operator_payload: dict[str, Any],
    draft_status: str,
    notification_severity: str,
) -> dict[str, Any]:
    system_snapshot = _mapping(operator_payload.get("system_snapshot"))
    governance = _mapping(operator_payload.get("parameter_governance"))
    pipeline = _mapping(operator_payload.get("pipeline_health"))
    freshness = _mapping(operator_payload.get("data_freshness"))
    alerts = _mapping(operator_payload.get("alerts"))
    brief_status = _brief_status_for_payload(operator_payload, draft_status)
    return {
        "brief_status": brief_status,
        "summary_level": (
            _string_value(operator_payload.get("summary_level")) or notification_severity
        ),
        "headline": _headline(operator_payload, draft_status),
        "can_trust_outputs_today": system_snapshot.get("can_trust_outputs_today") is True,
        "manual_action_required": system_snapshot.get("manual_action_required") is True,
        "parameter_governance_status": (
            _string_value(governance.get("status"))
            or _string_value(governance.get("digest_status"))
            or "UNKNOWN"
        ),
        "pipeline_health_status": (
            _string_value(pipeline.get("status"))
            or _string_value(pipeline.get("health_status"))
            or "UNKNOWN"
        ),
        "data_freshness_status": (
            _string_value(freshness.get("status"))
            or _string_value(freshness.get("freshness_status"))
            or "UNKNOWN"
        ),
        "critical_alert_count": len(_strings(alerts.get("critical"))),
        "warning_count": len(_strings(alerts.get("warnings"))),
    }


def _headline(operator_payload: dict[str, Any], draft_status: str) -> str:
    if draft_status == DRAFT_INPUT_MISSING:
        return "Operator brief input is missing. Notification draft requires manual review."
    if draft_status == DRAFT_INPUT_INVALID:
        return "Operator brief input is invalid. Notification draft requires manual review."
    if draft_status == DRAFT_SAFETY_BLOCKED:
        return "Notification draft safety check failed. Review the operator brief before sending."
    if draft_status == DRAFT_ERROR:
        return "Notification draft generation failed. Review run log."
    return (
        _string_value(operator_payload.get("headline"))
        or "Trading system status requires manual review."
    )


def _operator_brief_safety_validation(
    operator_payload: dict[str, Any],
    *,
    operator_status: str,
    operator_error: str,
) -> dict[str, Any]:
    blocking: list[str] = []
    if operator_status == STATUS_MISSING:
        blocking.append("Required TRADING-022 operator brief is missing.")
    elif operator_status == STATUS_INVALID:
        blocking.append(f"Required TRADING-022 operator brief is invalid: {operator_error}")

    task_id_valid = operator_payload.get("task_id") == INPUT_TASK_ID
    production_effect_none = operator_payload.get("production_effect") == PRODUCTION_EFFECT_NONE
    operator_brief_only = operator_payload.get("operator_brief_only") is True
    read_only = operator_payload.get("read_only") is True
    no_execution_flags = all(
        operator_payload.get(field) is False for field in OPERATOR_BRIEF_FALSE_FIELDS
    )

    if operator_status == STATUS_FOUND:
        if not task_id_valid:
            blocking.append("Operator brief task_id must be TRADING-022.")
        if not production_effect_none:
            blocking.append("Operator brief production_effect must be none.")
        if not operator_brief_only:
            blocking.append("Operator brief must have operator_brief_only=true.")
        if not read_only:
            blocking.append("Operator brief must have read_only=true.")
        for field in OPERATOR_BRIEF_FALSE_FIELDS:
            if operator_payload.get(field) is not False:
                blocking.append(f"Operator brief must have {field}=false.")

    status = "PASS" if not blocking else "FAIL"
    return {
        "status": status,
        "operator_brief_task_id_valid": task_id_valid,
        "operator_brief_production_effect_none": production_effect_none,
        "operator_brief_read_only": read_only,
        "operator_brief_operator_brief_only": operator_brief_only,
        "operator_brief_no_execution_flags": no_execution_flags,
        "no_notification_sent": True,
        "no_external_webhook_called": True,
        "no_pipeline_execution": True,
        "no_data_download": True,
        "no_apply_or_rollback": True,
        "no_broker_replay_trading": True,
        "blocking_reasons": blocking,
    }


def _input_artifacts(
    operator_path: Path,
    operator_status: str,
    optional_inputs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    result = {"operator_brief": _artifact_record(operator_path, operator_status)}
    for name, optional in optional_inputs.items():
        result[name] = _artifact_record(Path(optional["path"]), str(optional["status"]))
    return result


def _artifact_record(path: Path, status: str) -> dict[str, Any]:
    record = {"status": status, "path": str(path), "sha256": ""}
    if path.exists() and path.is_file():
        record["sha256"] = _sha256(path)
    return record


def _load_optional_inputs(
    *,
    as_of: date,
    data_root: Path,
    parameter_governance_digest_file: Path | None,
    pipeline_health_summary_file: Path | None,
    data_freshness_summary_file: Path | None,
    scheduler_dry_run_file: Path | None,
) -> dict[str, dict[str, Any]]:
    specs = {
        "parameter_governance_digest": (
            parameter_governance_digest_file,
            data_root / "derived" / "weight_iterations" / "governance" / "digests",
            "parameter_governance_daily_digest_",
        ),
        "pipeline_health_summary": (
            pipeline_health_summary_file,
            data_root / "derived" / "pipeline_health",
            "pipeline_health_summary_",
        ),
        "data_freshness_summary": (
            data_freshness_summary_file,
            data_root / "derived" / "data_freshness",
            "data_freshness_summary_",
        ),
        "scheduler_dry_run": (
            scheduler_dry_run_file,
            data_root / "derived" / "operator_briefs" / "scheduler_dry_run",
            "daily_operator_brief_scheduler_dry_run_",
        ),
    }
    result: dict[str, dict[str, Any]] = {}
    for name, (explicit, root, prefix) in specs.items():
        path = (
            Path(explicit) if explicit is not None else _latest_dated_artifact(root, prefix, as_of)
        )
        if not path.exists():
            status = STATUS_OPTIONAL_MISSING
            payload: dict[str, Any] = {}
            error = "missing"
        else:
            payload, raw_status, error = _read_json_object_with_status(path)
            status = (
                STATUS_OPTIONAL_FOUND if raw_status == STATUS_FOUND else STATUS_OPTIONAL_INVALID
            )
        result[name] = {"path": str(path), "status": status, "payload": payload, "error": error}
    return result


def _optional_input_warnings(optional_inputs: dict[str, dict[str, Any]]) -> list[str]:
    warnings = []
    for name, item in optional_inputs.items():
        if item.get("status") == STATUS_OPTIONAL_INVALID:
            warnings.append(f"Optional input {name} is invalid and was not used.")
    return warnings


def _resolve_operator_brief_path(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
) -> Path:
    if explicit_path is not None:
        return Path(explicit_path)
    root = data_root / "derived" / "operator_briefs"
    return _latest_dated_artifact(
        root,
        "daily_trading_system_operator_brief_",
        as_of,
        default_path=default_operator_brief_json_path(data_root, as_of),
    )


def _latest_dated_artifact(
    root: Path,
    prefix: str,
    as_of: date,
    *,
    default_path: Path | None = None,
) -> Path:
    default = default_path or root / f"{prefix}{as_of.isoformat()}.json"
    if not root.exists():
        return default
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*.json"):
        raw_date = path.stem.removeprefix(prefix)
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default
    return max(candidates, key=lambda item: item[0])[1]


def _email_subject(brief_status: str, as_of: date) -> str:
    return f"[Trading System] Daily Operator Brief - {brief_status} - {as_of.isoformat()}"


def _recommended_next_steps(severity: str) -> list[str]:
    if severity == SEVERITY_URGENT:
        return ["Inspect the operator brief now.", "Escalate only after manual source review."]
    if severity == SEVERITY_ACTION:
        return ["Review pending operator brief items.", "Confirm blockers before sending."]
    if severity == SEVERITY_BLOCKED:
        return ["Review safety validation before sending any notification."]
    if severity == SEVERITY_WATCH:
        return ["Continue monitoring.", "Review warning details if needed."]
    return [
        "Continue observation.",
        "Review the parameter governance web view if details are needed.",
    ]


def _error_bundle(
    *,
    as_of: date,
    data_root: Path,
    metadata_path: Path,
    summary_markdown_path: Path,
    email_path: Path,
    chat_path: Path,
    mobile_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_draft_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "slack_sent": False,
        "discord_sent": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_notification_draft": False,
        "pipelines_executed_by_notification_draft": False,
        "data_downloaded_by_notification_draft": False,
        "apply_executed_by_notification_draft": False,
        "rollback_executed_by_notification_draft": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "draft_status": DRAFT_ERROR,
        "notification_severity": SEVERITY_UNKNOWN,
        "headline": "Notification draft generation failed. Review run log.",
        "input_artifacts": {
            "operator_brief": _artifact_record(
                default_operator_brief_json_path(data_root, as_of),
                STATUS_MISSING,
            )
        },
        "source_snapshot": {
            "brief_status": DRAFT_ERROR,
            "summary_level": SEVERITY_UNKNOWN,
            "headline": "Notification draft generation failed. Review run log.",
            "can_trust_outputs_today": False,
            "manual_action_required": True,
            "parameter_governance_status": "UNKNOWN",
            "pipeline_health_status": "UNKNOWN",
            "data_freshness_status": "UNKNOWN",
            "critical_alert_count": 0,
            "warning_count": 1,
        },
        "draft_outputs": {
            "email_draft": {"path": str(email_path), "subject": _email_subject("ERROR", as_of)},
            "chat_draft": {"path": str(chat_path)},
            "mobile_summary": {"path": str(mobile_path)},
            "summary_markdown": {"path": str(summary_markdown_path)},
        },
        "notification_content_summary": {},
        "safety_validation": {
            "status": "FAIL",
            "operator_brief_task_id_valid": False,
            "operator_brief_production_effect_none": False,
            "operator_brief_read_only": False,
            "operator_brief_operator_brief_only": False,
            "operator_brief_no_execution_flags": False,
            "no_notification_sent": True,
            "no_external_webhook_called": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": [error],
        },
        "manual_review_required": {
            "required": True,
            "instructions": ["Review run log before sending any notification."],
        },
        "alerts": {"critical": [], "warnings": [error], "notes": []},
        "audit": {
            "created_by": "scripts/generate_operator_brief_notification_draft.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_notification_drafts": True,
        },
        "output_artifacts": {
            "metadata_json": {"path": str(metadata_path)},
            "summary_markdown": {"path": str(summary_markdown_path)},
            "email_draft": {"path": str(email_path)},
            "chat_draft": {"path": str(chat_path)},
            "mobile_summary": {"path": str(mobile_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
    }
    drafts = {
        "email_draft": "\n".join(
            [
                f"Subject: {_email_subject('ERROR', as_of)}",
                "",
                "# Daily Trading System Operator Brief",
                "",
                "## Notification Draft Safety Blocked",
                "",
                "Notification draft generation failed. No email was sent.",
                "",
            ]
        ),
        "chat_draft": "**Notification Draft Safety Blocked**\n\nDraft generation failed.\n",
        "mobile_summary": _render_mobile_summary(payload=payload),
        "summary_markdown": render_operator_brief_notification_summary_markdown(payload),
    }
    payload["notification_content_summary"] = _content_summary(drafts)
    _assert_notification_safety_invariants(payload)
    return {"payload": payload, **drafts}


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("output_artifacts"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "created_at": _isoformat_z(generated_at),
        "run_status": payload.get("draft_status"),
        "draft_status": payload.get("draft_status"),
        "notification_severity": payload.get("notification_severity"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_draft_only": True,
        "read_only": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "slack_sent": False,
        "discord_sent": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_notification_draft": False,
        "pipelines_executed_by_notification_draft": False,
        "data_downloaded_by_notification_draft": False,
        "apply_executed_by_notification_draft": False,
        "rollback_executed_by_notification_draft": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "notification_metadata_json": _mapping(outputs.get("metadata_json")).get("path"),
        "notification_summary_markdown": _mapping(outputs.get("summary_markdown")).get("path"),
    }


def _assert_notification_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-030 production_effect must be none")
    for field in SAFETY_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-030 {field} must be true")
    for field in SAFETY_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-030 {field} must be false")


def _read_json_object_with_status(path: Path) -> tuple[dict[str, Any], str, str]:
    if not path.exists():
        return {}, STATUS_MISSING, "missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, STATUS_INVALID, str(exc)
    if not isinstance(payload, dict):
        return {}, STATUS_INVALID, "JSON root is not an object"
    return payload, STATUS_FOUND, ""


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _redacted_strings(
    values: list[str],
    warnings: list[str],
    warning_keys: set[str],
    *,
    context: str,
) -> list[str]:
    return [_redact_text(value, warnings, warning_keys, context=context) for value in values]


def _record_sensitive_source_texts(
    operator_payload: dict[str, Any],
    *,
    redaction_warnings: list[str],
    warning_keys: set[str],
) -> None:
    alerts = _mapping(operator_payload.get("alerts"))
    for context, values in (
        ("operator brief critical alerts", _strings(alerts.get("critical"))),
        ("operator brief warning alerts", _strings(alerts.get("warnings"))),
        ("operator brief alert notes", _strings(alerts.get("notes"))),
    ):
        for value in values:
            _redact_text(
                value,
                redaction_warnings,
                warning_keys,
                context=context,
            )


def _redact_text(
    text: str,
    warnings: list[str],
    warning_keys: set[str],
    *,
    context: str,
) -> str:
    def replace(match: re.Match[str]) -> str:
        field = match.group(1)
        separator = match.group(2)
        key = f"{context}:{field.lower()}"
        if key not in warning_keys:
            warning_keys.add(key)
            warnings.append(f"Redacted sensitive field `{field}` from {context}.")
        return f"{field}{separator}<REDACTED>"

    return SENSITIVE_ASSIGNMENT_RE.sub(replace, text)


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if isinstance(value, tuple):
        return [str(item) for item in value if item is not None]
    if isinstance(value, str):
        return [value]
    return []


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _bool_text(value: Any) -> str:
    return "true" if value is True else "false"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _line_count(content: str) -> int:
    return len(content.rstrip("\n").splitlines()) if content else 0


def _artifact_label(name: str) -> str:
    return name.replace("_", " ").title()
