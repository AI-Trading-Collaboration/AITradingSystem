from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "operator_brief_notification_dispatch_preview"
TASK_ID = "TRADING-032"
TASK_NAME = "Operator Brief Notification Dry-run Dispatch Preview"
INPUT_PREFLIGHT_TASK_ID = "TRADING-031"
INPUT_OPERATOR_BRIEF_TASK_ID = "TRADING-022"
INPUT_NOTIFICATION_DRAFT_TASK_ID = "TRADING-030"
MODE = "dry_run"
PRODUCTION_EFFECT_NONE = "none"

DISPATCH_WOULD_SEND = "WOULD_SEND"
DISPATCH_NEEDS_APPROVAL = "NEEDS_APPROVAL"
DISPATCH_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DISPATCH_BLOCKED = "BLOCKED"
DISPATCH_NOOP = "NOOP"

PREFLIGHT_PASS = "PASS"
PREFLIGHT_PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"
PREFLIGHT_NEEDS_APPROVAL = "NEEDS_APPROVAL"
PREFLIGHT_SAFETY_BLOCKED = "SAFETY_BLOCKED"
PREFLIGHT_BLOCKED = "BLOCKED"
PREFLIGHT_INPUT_MISSING = "INPUT_MISSING"
PREFLIGHT_INPUT_INVALID = "INPUT_INVALID"
PREFLIGHT_ERROR = "ERROR"

READINESS_READY = "READY_FOR_MANUAL_REVIEW"
READINESS_NEEDS_APPROVAL = "NEEDS_APPROVAL"
READINESS_BLOCKED = "BLOCKED"
READINESS_SAFETY_BLOCKED = "SAFETY_BLOCKED"

STATUS_FOUND = "FOUND"
STATUS_MISSING = "MISSING"
STATUS_INVALID = "INVALID"
STATUS_UNSAFE = "UNSAFE"

BODY_EXCERPT_MAX_CHARS = 500
# Conservative manual-review boundary for unusually large dry-run notification bodies.
# It affects only TRADING-032 preview approval state, not investment scoring or trading.
BODY_APPROVAL_REVIEW_THRESHOLD_CHARS = 8000

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

SAFETY_TRUE_FIELDS = (
    "manual_review_only",
    "dispatch_preview_only",
    "read_only",
    "safe_for_scheduler",
)
SAFETY_FALSE_FIELDS = (
    "external_side_effects",
    "network_access_required",
    "secrets_required",
    "email_sent",
    "gmail_draft_created",
    "gmail_draft_modified",
    "slack_sent",
    "telegram_sent",
    "discord_sent",
    "webhook_called",
    "mobile_push_sent",
    "operator_brief_executed_by_dispatch_preview",
    "notification_draft_executed_by_dispatch_preview",
    "delivery_preflight_executed_by_dispatch_preview",
    "pipelines_executed_by_dispatch_preview",
    "data_downloaded_by_dispatch_preview",
    "apply_executed_by_dispatch_preview",
    "rollback_executed_by_dispatch_preview",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)

SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"\b(api_key|apiKey|secret|token|password|credential|broker_token|account_id|"
    r"private_key)\b\s*([:=])\s*(\"[^\"]*\"|'[^']*'|\[[^\]]+\]|[^\s,;`]+)",
    re.IGNORECASE,
)
PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----", re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s)>\]]+", re.IGNORECASE)


def default_notification_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs" / "notifications"


def default_dispatch_preview_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "dispatch_preview"


def default_dispatch_preview_json_path(data_root: Path, as_of: date) -> Path:
    return default_dispatch_preview_root(data_root) / (
        f"operator_brief_notification_dispatch_preview_{as_of.isoformat()}.json"
    )


def default_dispatch_preview_markdown_path(data_root: Path, as_of: date) -> Path:
    return default_dispatch_preview_json_path(data_root, as_of).with_suffix(".md")


def default_dispatch_preview_latest_json_path(data_root: Path) -> Path:
    return default_dispatch_preview_root(data_root) / "latest.json"


def default_dispatch_preview_latest_markdown_path(data_root: Path) -> Path:
    return default_dispatch_preview_root(data_root) / "latest.md"


def default_dispatch_preview_run_log_path(data_root: Path) -> Path:
    return default_dispatch_preview_root(data_root) / "run.log"


def write_operator_brief_notification_dispatch_preview(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    input_preflight_file: Path | None = None,
    operator_brief_json_file: Path | None = None,
    operator_brief_markdown_file: Path | None = None,
    notification_draft_metadata_file: Path | None = None,
    output_dir: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = _normalize_data_root(data_root)
    project_root = _project_root_from_data_root(data_root)
    output_root, output_safety_finding = _resolve_output_root(
        data_root=data_root,
        project_root=project_root,
        output_dir=output_dir,
    )
    json_path = (
        output_root / f"operator_brief_notification_dispatch_preview_{as_of.isoformat()}.json"
    )
    markdown_path = json_path.with_suffix(".md")
    latest_json_path = output_root / "latest.json"
    latest_markdown_path = output_root / "latest.md"
    run_log_path = output_root / "run.log"

    payload = build_operator_brief_notification_dispatch_preview(
        as_of=as_of,
        data_root=data_root,
        input_preflight_file=input_preflight_file,
        operator_brief_json_file=operator_brief_json_file,
        operator_brief_markdown_file=operator_brief_markdown_file,
        notification_draft_metadata_file=notification_draft_metadata_file,
        output_json_path=json_path,
        output_markdown_path=markdown_path,
        latest_json_path=latest_json_path,
        latest_markdown_path=latest_markdown_path,
        run_log_path=run_log_path,
        generated_at=generated,
        output_safety_finding=output_safety_finding,
    )
    markdown = render_operator_brief_notification_dispatch_preview_markdown(payload)
    run_log = render_operator_brief_notification_dispatch_preview_run_log(payload)
    _write_json(json_path, payload)
    _write_text(markdown_path, markdown)
    _write_json(latest_json_path, payload)
    _write_text(latest_markdown_path, markdown)
    _write_text(run_log_path, run_log)
    return payload


def build_operator_brief_notification_dispatch_preview(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    input_preflight_file: Path | None = None,
    operator_brief_json_file: Path | None = None,
    operator_brief_markdown_file: Path | None = None,
    notification_draft_metadata_file: Path | None = None,
    output_json_path: Path | None = None,
    output_markdown_path: Path | None = None,
    latest_json_path: Path | None = None,
    latest_markdown_path: Path | None = None,
    run_log_path: Path | None = None,
    generated_at: datetime | None = None,
    output_safety_finding: str | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = _normalize_data_root(data_root)
    project_root = _project_root_from_data_root(data_root)
    output_json_path = output_json_path or default_dispatch_preview_json_path(data_root, as_of)
    output_markdown_path = output_markdown_path or output_json_path.with_suffix(".md")
    latest_json_path = latest_json_path or default_dispatch_preview_latest_json_path(data_root)
    latest_markdown_path = latest_markdown_path or default_dispatch_preview_latest_markdown_path(
        data_root
    )
    run_log_path = run_log_path or default_dispatch_preview_run_log_path(data_root)

    path_safety_findings: list[str] = []
    if output_safety_finding:
        path_safety_findings.append(output_safety_finding)

    preflight_path, preflight_path_finding = _resolve_preflight_path(
        as_of=as_of,
        data_root=data_root,
        project_root=project_root,
        explicit_path=input_preflight_file,
    )
    if preflight_path_finding:
        path_safety_findings.append(preflight_path_finding)
    preflight_payload, preflight_artifact_status, preflight_error = _read_json_object_with_status(
        preflight_path,
        unsafe=bool(preflight_path_finding),
    )

    operator_json_path, operator_json_path_finding = _resolve_operator_brief_json_path(
        as_of=as_of,
        data_root=data_root,
        project_root=project_root,
        explicit_path=operator_brief_json_file,
    )
    if operator_json_path_finding:
        path_safety_findings.append(operator_json_path_finding)
    operator_payload, operator_status, operator_error = _read_json_object_with_status(
        operator_json_path,
        unsafe=bool(operator_json_path_finding),
    )

    operator_md_path, operator_md_path_finding = _resolve_operator_brief_markdown_path(
        data_root=data_root,
        project_root=project_root,
        operator_json_path=operator_json_path,
        explicit_path=operator_brief_markdown_file,
    )
    if operator_md_path_finding:
        path_safety_findings.append(operator_md_path_finding)
    operator_markdown, operator_markdown_status, operator_markdown_error = _read_text_with_status(
        operator_md_path,
        unsafe=bool(operator_md_path_finding),
    )

    draft_metadata_path, draft_metadata_path_finding = _resolve_notification_draft_metadata_path(
        as_of=as_of,
        data_root=data_root,
        project_root=project_root,
        explicit_path=notification_draft_metadata_file,
        preflight_payload=preflight_payload,
    )
    if draft_metadata_path_finding:
        path_safety_findings.append(draft_metadata_path_finding)
    draft_payload, draft_status, draft_error = _read_json_object_with_status(
        draft_metadata_path,
        unsafe=bool(draft_metadata_path_finding),
    )

    artifact_path_findings = _artifact_path_safety_findings(
        payloads=(preflight_payload, operator_payload, draft_payload),
        project_root=project_root,
        data_root=data_root,
    )
    path_safety_findings.extend(artifact_path_findings)

    draft_texts = _load_draft_texts(
        draft_payload=draft_payload,
        data_root=data_root,
        project_root=project_root,
        path_safety_findings=path_safety_findings,
    )
    email_body = draft_texts["email_draft"]["content"]
    body_source = email_body or operator_markdown or _operator_brief_json_body(operator_payload)
    subject_preview = _subject_preview(
        draft_payload=draft_payload,
        email_body=email_body,
        as_of=as_of,
    )
    title_preview = _title_preview(
        operator_payload=operator_payload,
        operator_markdown=operator_markdown,
        as_of=as_of,
    )
    sensitive_flags = _unique_strings(
        [
            *path_safety_findings,
            *_sensitive_content_flags(subject_preview, label="subject"),
            *_sensitive_content_flags(title_preview, label="title"),
            *_sensitive_content_flags(body_source, label="body"),
            *_url_safety_flags(preflight_payload),
        ]
    )
    redacted_subject = _redact_sensitive_text(_mask_recipient(subject_preview))
    redacted_title = _redact_sensitive_text(_mask_recipient(title_preview))
    redacted_body = _redact_sensitive_text(_mask_recipient(body_source))
    message = {
        "subject_preview": redacted_subject,
        "title_preview": redacted_title,
        "body_excerpt": _body_excerpt(redacted_body),
        "body_length": len(body_source),
        "contains_markdown": _contains_markdown(body_source),
    }

    preflight_summary_status = _preflight_summary_status(
        preflight_payload=preflight_payload,
        preflight_artifact_status=preflight_artifact_status,
    )
    preflight_reasons = _preflight_reasons(
        preflight_payload=preflight_payload,
        preflight_error=preflight_error,
    )
    preflight_warnings = _preflight_warnings(preflight_payload=preflight_payload)
    channels = _channel_preview_records(preflight_payload=preflight_payload)
    noop_reason = _noop_reason(
        preflight_payload=preflight_payload,
        operator_payload=operator_payload,
        draft_payload=draft_payload,
        channels=channels,
    )
    block_reasons = _blocked_reasons(
        preflight_summary_status=preflight_summary_status,
        preflight_artifact_status=preflight_artifact_status,
        preflight_error=preflight_error,
        operator_status=operator_status,
        operator_error=operator_error,
        operator_markdown_status=operator_markdown_status,
        operator_markdown_error=operator_markdown_error,
        draft_status=draft_status,
        draft_error=draft_error,
        message=message,
        channels=channels,
    )
    approval_reasons = _approval_reasons(
        preflight_payload=preflight_payload,
        operator_payload=operator_payload,
        body_length=message["body_length"],
    )
    final_status = _final_dispatch_status(
        noop_reason=noop_reason,
        sensitive_flags=sensitive_flags,
        preflight_summary_status=preflight_summary_status,
        block_reasons=block_reasons,
        approval_reasons=approval_reasons,
        channels=channels,
    )
    channels = _mark_would_send(channels, final_status=final_status)
    next_recommended_action = _next_recommended_action(
        final_status=final_status,
        noop_reason=noop_reason,
    )
    human_action_required = final_status != DISPATCH_NOOP

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "dispatch_preview_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "external_side_effects": False,
        "network_access_required": False,
        "secrets_required": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "telegram_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_dispatch_preview": False,
        "notification_draft_executed_by_dispatch_preview": False,
        "delivery_preflight_executed_by_dispatch_preview": False,
        "pipelines_executed_by_dispatch_preview": False,
        "data_downloaded_by_dispatch_preview": False,
        "apply_executed_by_dispatch_preview": False,
        "rollback_executed_by_dispatch_preview": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": TASK_ID,
            "task_name": TASK_NAME,
            "run_date": as_of.isoformat(),
            "generated_at": _isoformat_z(generated),
            "preview_generated_at": _isoformat_z(generated),
            "mode": MODE,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
        },
        "input_refs": {
            "preflight_artifact": _input_ref(preflight_path, preflight_artifact_status),
            "operator_brief_json": _input_ref(operator_json_path, operator_status),
            "operator_brief_markdown": _input_ref(
                operator_md_path,
                operator_markdown_status,
            ),
            "notification_draft_metadata": _input_ref(draft_metadata_path, draft_status),
            "template_refs": _template_refs(draft_payload=draft_payload, draft_texts=draft_texts),
        },
        "preflight_summary": {
            "status": preflight_summary_status,
            "allowed_to_dispatch": final_status == DISPATCH_WOULD_SEND,
            "reasons": _unique_strings([*preflight_reasons, *block_reasons, *approval_reasons]),
            "warnings": preflight_warnings,
        },
        "dispatch_preview": {
            "dispatch_status": final_status,
            "channels": channels,
            "message": message,
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "recipient_masking_applied": True,
            "sensitive_content_flags": sensitive_flags,
        },
        "decision": {
            "final_status": final_status,
            "human_action_required": human_action_required,
            "next_recommended_action": next_recommended_action,
        },
        "audit": {
            "created_by": "scripts/run_operator_brief_notification_dispatch_preview.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_dispatch_preview_artifacts": True,
        },
        "output_artifacts": {
            "dispatch_preview_json": {"path": str(output_json_path)},
            "dispatch_preview_markdown": {"path": str(output_markdown_path)},
            "latest_json": {"path": str(latest_json_path)},
            "latest_markdown": {"path": str(latest_markdown_path)},
            "run_log": {"path": str(run_log_path)},
        },
    }
    _assert_dispatch_preview_safety_invariants(payload)
    return payload


def render_operator_brief_notification_dispatch_preview_markdown(
    payload: dict[str, Any],
) -> str:
    metadata = _mapping(payload.get("metadata"))
    preflight = _mapping(payload.get("preflight_summary"))
    preview = _mapping(payload.get("dispatch_preview"))
    message = _mapping(preview.get("message"))
    safety = _mapping(payload.get("safety"))
    decision = _mapping(payload.get("decision"))
    input_refs = _mapping(payload.get("input_refs"))
    lines = [
        "# Operator Brief Notification Dispatch Preview",
        "",
        "## Metadata",
        "",
        f"- Task: `{metadata.get('task_id', TASK_ID)}`",
        f"- Mode: `{metadata.get('mode', MODE)}`",
        f"- Production effect: `{metadata.get('production_effect', PRODUCTION_EFFECT_NONE)}`",
        f"- Manual review only: `{_bool_text(metadata.get('manual_review_only'))}`",
        f"- Run date: `{metadata.get('run_date', payload.get('date'))}`",
        "",
        "## Final Decision",
        "",
        f"- Status: `{decision.get('final_status', DISPATCH_BLOCKED)}`",
        f"- Human action required: `{_bool_text(decision.get('human_action_required'))}`",
        f"- Next recommended action: {decision.get('next_recommended_action') or ''}",
        "",
        "## Preflight Summary",
        "",
        f"- Preflight status: `{preflight.get('status', DISPATCH_BLOCKED)}`",
        f"- Allowed to dispatch: `{_bool_text(preflight.get('allowed_to_dispatch'))}`",
        "- Reasons:",
        *_markdown_list(_strings(preflight.get("reasons"))),
        "- Warnings:",
        *_markdown_list(_strings(preflight.get("warnings"))),
        "",
        "## Dispatch Preview",
        "",
    ]
    channels = list(_records(preview.get("channels")))
    if not channels:
        lines.append("- No channels available.")
    for index, channel in enumerate(channels, start=1):
        lines.extend(
            [
                f"### Channel {index}",
                "",
                f"- Type: `{channel.get('channel_type', 'unknown')}`",
                f"- Target: `{channel.get('target_ref', 'unknown')}`",
                f"- Enabled: `{_bool_text(channel.get('enabled'))}`",
                f"- Would send: `{_bool_text(channel.get('would_send'))}`",
                f"- Reason: {channel.get('reason') or ''}",
                "",
            ]
        )
    lines.extend(
        [
            "## Message Preview",
            "",
            f"- Subject: {message.get('subject_preview') or ''}",
            f"- Title: {message.get('title_preview') or ''}",
            f"- Body length: `{message.get('body_length', 0)}`",
            "- Body excerpt:",
            "",
            f"> {str(message.get('body_excerpt') or '').replace(chr(10), chr(10) + '> ')}",
            "",
            "## Safety",
            "",
            f"- External side effects: `{_bool_text(safety.get('external_side_effects'))}`",
            f"- Network access required: `{_bool_text(safety.get('network_access_required'))}`",
            f"- Secrets required: `{_bool_text(safety.get('secrets_required'))}`",
            "- Recipient masking applied: "
            f"`{_bool_text(safety.get('recipient_masking_applied'))}`",
            "- Sensitive content flags:",
            *_markdown_list(_strings(safety.get("sensitive_content_flags"))),
            "",
            "## Input Artifacts",
            "",
            f"- Preflight artifact: `{_path_from_ref(input_refs.get('preflight_artifact'))}`",
            f"- Operator Brief JSON: `{_path_from_ref(input_refs.get('operator_brief_json'))}`",
            "- Operator Brief Markdown: "
            f"`{_path_from_ref(input_refs.get('operator_brief_markdown'))}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_operator_brief_notification_dispatch_preview_run_log(
    payload: dict[str, Any],
) -> str:
    decision = _mapping(payload.get("decision"))
    preview = _mapping(payload.get("dispatch_preview"))
    outputs = _mapping(payload.get("output_artifacts"))
    return "\n".join(
        [
            f"Operator Brief Notification Dispatch Preview Run - {payload.get('date')}",
            "run_status=COMPLETED",
            f"final_status={decision.get('final_status')}",
            f"dispatch_status={preview.get('dispatch_status')}",
            f"production_effect={payload.get('production_effect')}",
            f"manual_review_only={_bool_text(payload.get('manual_review_only'))}",
            f"dispatch_preview_only={_bool_text(payload.get('dispatch_preview_only'))}",
            f"read_only={_bool_text(payload.get('read_only'))}",
            f"external_side_effects={_bool_text(payload.get('external_side_effects'))}",
            f"network_access_required={_bool_text(payload.get('network_access_required'))}",
            f"secrets_required={_bool_text(payload.get('secrets_required'))}",
            f"email_sent={_bool_text(payload.get('email_sent'))}",
            f"gmail_draft_created={_bool_text(payload.get('gmail_draft_created'))}",
            f"gmail_draft_modified={_bool_text(payload.get('gmail_draft_modified'))}",
            f"slack_sent={_bool_text(payload.get('slack_sent'))}",
            f"telegram_sent={_bool_text(payload.get('telegram_sent'))}",
            f"discord_sent={_bool_text(payload.get('discord_sent'))}",
            f"webhook_called={_bool_text(payload.get('webhook_called'))}",
            f"mobile_push_sent={_bool_text(payload.get('mobile_push_sent'))}",
            f"broker_execution={_bool_text(payload.get('broker_execution'))}",
            f"replay_execution={_bool_text(payload.get('replay_execution'))}",
            f"trading_execution={_bool_text(payload.get('trading_execution'))}",
            f"dispatch_preview_json={_path_from_ref(outputs.get('dispatch_preview_json'))}",
            f"dispatch_preview_markdown={_path_from_ref(outputs.get('dispatch_preview_markdown'))}",
            f"latest_json={_path_from_ref(outputs.get('latest_json'))}",
            f"latest_markdown={_path_from_ref(outputs.get('latest_markdown'))}",
            f"run_log={_path_from_ref(outputs.get('run_log'))}",
            "",
        ]
    )


def should_fail_cli(payload: dict[str, Any]) -> bool:
    return payload.get("runtime_error") is True


def _resolve_preflight_path(
    *,
    as_of: date,
    data_root: Path,
    project_root: Path,
    explicit_path: Path | None,
) -> tuple[Path, str | None]:
    if explicit_path is not None:
        return _resolve_repo_path(
            value=explicit_path,
            data_root=data_root,
            project_root=project_root,
            purpose="preflight artifact",
        )
    preflight_root = default_notification_root(data_root) / "delivery_preflight"
    default_path = preflight_root / (
        f"operator_brief_notification_delivery_preflight_{as_of.isoformat()}.json"
    )
    return (
        _latest_dated_or_latest(
            root=preflight_root,
            prefix="operator_brief_notification_delivery_preflight_",
            as_of=as_of,
            default_path=default_path,
            latest_name="latest.json",
        ),
        None,
    )


def _resolve_operator_brief_json_path(
    *,
    as_of: date,
    data_root: Path,
    project_root: Path,
    explicit_path: Path | None,
) -> tuple[Path, str | None]:
    if explicit_path is not None:
        return _resolve_repo_path(
            value=explicit_path,
            data_root=data_root,
            project_root=project_root,
            purpose="operator brief json",
        )
    root = data_root / "derived" / "operator_briefs"
    default_path = root / f"daily_trading_system_operator_brief_{as_of.isoformat()}.json"
    return (
        _latest_dated_or_latest(
            root=root,
            prefix="daily_trading_system_operator_brief_",
            as_of=as_of,
            default_path=default_path,
            latest_name="latest.json",
        ),
        None,
    )


def _resolve_operator_brief_markdown_path(
    *,
    data_root: Path,
    project_root: Path,
    operator_json_path: Path,
    explicit_path: Path | None,
) -> tuple[Path, str | None]:
    if explicit_path is not None:
        return _resolve_repo_path(
            value=explicit_path,
            data_root=data_root,
            project_root=project_root,
            purpose="operator brief markdown",
        )
    return operator_json_path.with_suffix(".md"), None


def _resolve_notification_draft_metadata_path(
    *,
    as_of: date,
    data_root: Path,
    project_root: Path,
    explicit_path: Path | None,
    preflight_payload: dict[str, Any],
) -> tuple[Path, str | None]:
    if explicit_path is not None:
        return _resolve_repo_path(
            value=explicit_path,
            data_root=data_root,
            project_root=project_root,
            purpose="notification draft metadata",
        )
    artifact_path = _string_value(
        _mapping(
            _mapping(preflight_payload.get("input_artifacts")).get("notification_draft_metadata")
        ).get("path")
    )
    if artifact_path:
        return _resolve_repo_path(
            value=Path(artifact_path),
            data_root=data_root,
            project_root=project_root,
            purpose="notification draft metadata",
        )
    root = default_notification_root(data_root)
    default_path = root / f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    return (
        _latest_dated_or_latest(
            root=root,
            prefix="operator_brief_notification_draft_",
            as_of=as_of,
            default_path=default_path,
            latest_name="latest.json",
        ),
        None,
    )


def _latest_dated_or_latest(
    *,
    root: Path,
    prefix: str,
    as_of: date,
    default_path: Path,
    latest_name: str,
) -> Path:
    if not root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*.json"):
        raw_date = path.stem.removeprefix(prefix)
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if candidates:
        return max(candidates, key=lambda item: item[0])[1]
    latest = root / latest_name
    return latest if latest.exists() else default_path


def _resolve_output_root(
    *,
    data_root: Path,
    project_root: Path,
    output_dir: Path | None,
) -> tuple[Path, str | None]:
    if output_dir is None:
        return default_dispatch_preview_root(data_root), None
    path, finding = _resolve_repo_path(
        value=output_dir,
        data_root=data_root,
        project_root=project_root,
        purpose="dispatch preview output directory",
        allow_missing=True,
    )
    if finding:
        return default_dispatch_preview_root(data_root), finding
    return path, None


def _resolve_repo_path(
    *,
    value: Path,
    data_root: Path,
    project_root: Path,
    purpose: str,
    allow_missing: bool = True,
) -> tuple[Path, str | None]:
    path_value = Path(value)
    if path_value.is_absolute():
        candidate = path_value
    elif path_value.parts and path_value.parts[0] == data_root.name:
        candidate = project_root / path_value
    else:
        project_candidate = project_root / path_value
        data_candidate = data_root / path_value
        candidate = (
            project_candidate if project_candidate.exists() or allow_missing else data_candidate
        )
    resolved = candidate.resolve(strict=False)
    project_resolved = project_root.resolve(strict=False)
    if not _is_relative_to(resolved, project_resolved):
        return resolved, f"{purpose} path escapes repo root: {resolved}"
    if any(part.lower().startswith(".env") for part in resolved.parts):
        return resolved, f"{purpose} path points to .env secrets and was not read: {resolved}"
    return resolved, None


def _read_json_object_with_status(
    path: Path,
    *,
    unsafe: bool = False,
) -> tuple[dict[str, Any], str, str | None]:
    if unsafe:
        return {}, STATUS_UNSAFE, f"Unsafe JSON input path was blocked: {path}"
    if not path.exists():
        return {}, STATUS_MISSING, f"JSON input not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, STATUS_INVALID, f"JSON input invalid: {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, STATUS_INVALID, f"JSON input must be an object: {path}"
    return payload, STATUS_FOUND, None


def _read_text_with_status(
    path: Path,
    *,
    unsafe: bool = False,
) -> tuple[str, str, str | None]:
    if unsafe:
        return "", STATUS_UNSAFE, f"Unsafe text input path was blocked: {path}"
    if not path.exists():
        return "", STATUS_MISSING, f"Text input not found: {path}"
    try:
        return path.read_text(encoding="utf-8"), STATUS_FOUND, None
    except OSError as exc:
        return "", STATUS_INVALID, f"Text input invalid: {path}: {exc}"


def _load_draft_texts(
    *,
    draft_payload: dict[str, Any],
    data_root: Path,
    project_root: Path,
    path_safety_findings: list[str],
) -> dict[str, dict[str, Any]]:
    outputs = _mapping(draft_payload.get("draft_outputs"))
    return {
        "email_draft": _load_draft_text(
            label="email_draft",
            path_value=_mapping(outputs.get("email_draft")).get("path"),
            data_root=data_root,
            project_root=project_root,
            path_safety_findings=path_safety_findings,
        ),
        "chat_draft": _load_draft_text(
            label="chat_draft",
            path_value=_mapping(outputs.get("chat_draft")).get("path"),
            data_root=data_root,
            project_root=project_root,
            path_safety_findings=path_safety_findings,
        ),
        "mobile_summary": _load_draft_text(
            label="mobile_summary",
            path_value=_mapping(outputs.get("mobile_summary")).get("path"),
            data_root=data_root,
            project_root=project_root,
            path_safety_findings=path_safety_findings,
        ),
    }


def _load_draft_text(
    *,
    label: str,
    path_value: object,
    data_root: Path,
    project_root: Path,
    path_safety_findings: list[str],
) -> dict[str, Any]:
    path_text = _string_value(path_value)
    if not path_text:
        return {"status": STATUS_MISSING, "path": None, "sha256": None, "content": ""}
    path, finding = _resolve_repo_path(
        value=Path(path_text),
        data_root=data_root,
        project_root=project_root,
        purpose=f"{label} draft",
    )
    if finding:
        path_safety_findings.append(finding)
        return {"status": STATUS_UNSAFE, "path": str(path), "sha256": None, "content": ""}
    content, status, _error = _read_text_with_status(path)
    return {
        "status": status,
        "path": str(path),
        "sha256": _sha256_path(path) if status == STATUS_FOUND else None,
        "content": content,
    }


def _preflight_summary_status(
    *,
    preflight_payload: dict[str, Any],
    preflight_artifact_status: str,
) -> str:
    if preflight_artifact_status == STATUS_UNSAFE:
        return PREFLIGHT_SAFETY_BLOCKED
    if (
        preflight_artifact_status != STATUS_FOUND
        or preflight_payload.get("report_type") != "operator_brief_notification_delivery_preflight"
        or preflight_payload.get("task_id") != INPUT_PREFLIGHT_TASK_ID
    ):
        return PREFLIGHT_BLOCKED
    raw_status = _string_value(preflight_payload.get("preflight_status")) or PREFLIGHT_BLOCKED
    readiness = _string_value(preflight_payload.get("delivery_readiness"))
    approval = _mapping(preflight_payload.get("approval_validation"))
    if raw_status == PREFLIGHT_SAFETY_BLOCKED or readiness == READINESS_SAFETY_BLOCKED:
        return PREFLIGHT_SAFETY_BLOCKED
    if raw_status in {
        PREFLIGHT_BLOCKED,
        PREFLIGHT_INPUT_MISSING,
        PREFLIGHT_INPUT_INVALID,
        PREFLIGHT_ERROR,
    }:
        return PREFLIGHT_BLOCKED
    if readiness == READINESS_NEEDS_APPROVAL or approval.get("approval_required") is True:
        return PREFLIGHT_NEEDS_APPROVAL
    return PREFLIGHT_PASS


def _preflight_reasons(
    *,
    preflight_payload: dict[str, Any],
    preflight_error: str | None,
) -> list[str]:
    reasons: list[str] = []
    if preflight_error:
        reasons.append(preflight_error)
    alerts = _mapping(preflight_payload.get("alerts"))
    reasons.extend(_strings(alerts.get("critical")))
    reasons.extend(
        _strings(_mapping(preflight_payload.get("draft_validation")).get("blocking_reasons"))
    )
    reasons.extend(
        _strings(_mapping(preflight_payload.get("approval_validation")).get("blocking_reasons"))
    )
    approval_reason = _string_value(
        _mapping(preflight_payload.get("approval_validation")).get("approval_reason")
    )
    if approval_reason:
        reasons.append(approval_reason)
    return _unique_strings(reasons)


def _preflight_warnings(*, preflight_payload: dict[str, Any]) -> list[str]:
    alerts = _mapping(preflight_payload.get("alerts"))
    warnings = [*_strings(alerts.get("warnings"))]
    warnings.extend(
        _strings(_mapping(preflight_payload.get("approval_validation")).get("warnings"))
    )
    return _unique_strings(warnings)


def _channel_preview_records(*, preflight_payload: dict[str, Any]) -> list[dict[str, Any]]:
    readiness = _mapping(preflight_payload.get("channel_readiness"))
    records: list[dict[str, Any]] = []
    for channel_id, raw_record in readiness.items():
        if not isinstance(channel_id, str):
            continue
        record = _mapping(raw_record)
        enabled = _channel_enabled(record)
        target = _channel_target(channel_id=channel_id, record=record)
        records.append(
            {
                "channel_id": channel_id,
                "channel_type": _channel_type(channel_id),
                "target_ref": _mask_recipient(target),
                "enabled": enabled,
                "would_send": False,
                "reason": _channel_reason(channel_id=channel_id, record=record, enabled=enabled),
            }
        )
    return records


def _channel_enabled(record: dict[str, Any]) -> bool:
    if record.get("enabled") is False:
        return False
    reasons = " ".join(_strings(record.get("blocking_reasons"))).lower()
    if "disabled" in reasons:
        return False
    status = _string_value(record.get("status"))
    draft_available = record.get("draft_available") is not False
    return draft_available and status in {READINESS_READY, PREFLIGHT_PASS, "READY"}


def _channel_target(*, channel_id: str, record: dict[str, Any]) -> str:
    for key in ("target_ref", "recipient", "recipient_ref", "target", "to"):
        value = _string_value(record.get(key))
        if value:
            return value
    if channel_id == "email":
        return "operator-email-recipient"
    if channel_id == "chat":
        return "operator-chat-channel"
    if channel_id == "mobile":
        return "operator-mobile-review-target"
    return f"operator-{channel_id}-target"


def _channel_type(channel_id: str) -> str:
    if channel_id == "email":
        return "email"
    if channel_id in {"webhook", "slack", "telegram", "discord"}:
        return "webhook"
    if channel_id in {"chat", "mobile", "file"}:
        return "file"
    return "unknown"


def _channel_reason(*, channel_id: str, record: dict[str, Any], enabled: bool) -> str:
    reasons = _strings(record.get("blocking_reasons"))
    warnings = _strings(record.get("warnings"))
    if reasons:
        return "；".join(reasons)
    if enabled:
        return f"{channel_id} channel is enabled for dry-run preview only."
    if warnings:
        return "；".join(warnings)
    return f"{channel_id} channel is not enabled."


def _mark_would_send(
    channels: list[dict[str, Any]],
    *,
    final_status: str,
) -> list[dict[str, Any]]:
    marked: list[dict[str, Any]] = []
    for channel in channels:
        record = dict(channel)
        record["would_send"] = final_status == DISPATCH_WOULD_SEND and record.get("enabled") is True
        if final_status != DISPATCH_WOULD_SEND and record.get("enabled") is True:
            record["reason"] = (
                f"{record.get('reason')} Dispatch preview final status is {final_status}."
            )
        marked.append(record)
    return marked


def _noop_reason(
    *,
    preflight_payload: dict[str, Any],
    operator_payload: dict[str, Any],
    draft_payload: dict[str, Any],
    channels: list[dict[str, Any]],
) -> str | None:
    for payload in (preflight_payload, operator_payload, draft_payload):
        if payload.get("notification_enabled") is False or payload.get("dispatch_enabled") is False:
            return "Notification is explicitly disabled by upstream artifact."
    brief_status = _string_value(operator_payload.get("brief_status")).upper()
    if brief_status in {"NO_REPORT", "NO_TRADING_DAY", "NO_UPDATE", "NOOP"}:
        return f"Operator brief upstream status is {brief_status}."
    if channels and not any(channel.get("enabled") is True for channel in channels):
        joined_reasons = " ".join(str(channel.get("reason") or "").lower() for channel in channels)
        if "disabled" in joined_reasons:
            return "All notification channels are explicitly disabled."
    return None


def _blocked_reasons(
    *,
    preflight_summary_status: str,
    preflight_artifact_status: str,
    preflight_error: str | None,
    operator_status: str,
    operator_error: str | None,
    operator_markdown_status: str,
    operator_markdown_error: str | None,
    draft_status: str,
    draft_error: str | None,
    message: dict[str, Any],
    channels: list[dict[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if preflight_summary_status == PREFLIGHT_BLOCKED:
        reasons.append("TRADING-031 preflight is blocked or unavailable.")
    if preflight_artifact_status != STATUS_FOUND and preflight_error:
        reasons.append(preflight_error)
    if operator_status != STATUS_FOUND:
        reasons.append(operator_error or "Operator brief JSON is missing or invalid.")
    if operator_markdown_status not in {STATUS_FOUND, STATUS_MISSING}:
        reasons.append(operator_markdown_error or "Operator brief Markdown is invalid.")
    if draft_status not in {STATUS_FOUND, STATUS_MISSING}:
        reasons.append(draft_error or "Notification draft metadata is invalid.")
    if not _string_value(message.get("body_excerpt")):
        reasons.append("Message preview body excerpt could not be generated.")
    if not channels:
        reasons.append("No notification channels were available from TRADING-031 preflight.")
    elif not any(channel.get("enabled") is True for channel in channels):
        reasons.append("No notification channel is enabled for dispatch preview.")
    return _unique_strings(reasons)


def _approval_reasons(
    *,
    preflight_payload: dict[str, Any],
    operator_payload: dict[str, Any],
    body_length: int,
) -> list[str]:
    reasons: list[str] = []
    approval = _mapping(preflight_payload.get("approval_validation"))
    readiness = _string_value(preflight_payload.get("delivery_readiness"))
    severity = _string_value(preflight_payload.get("notification_severity"))
    brief_status = _string_value(operator_payload.get("brief_status"))
    if readiness == READINESS_NEEDS_APPROVAL or approval.get("approval_required") is True:
        reasons.append("TRADING-031 preflight requires approval before dispatch.")
    if severity in {"ACTION", "URGENT"} or brief_status in {"ACTION_REQUIRED", "URGENT"}:
        reasons.append("Operator brief or notification severity requires human approval.")
    if body_length > BODY_APPROVAL_REVIEW_THRESHOLD_CHARS:
        reasons.append("Message body exceeds the dry-run manual review threshold.")
    return _unique_strings(reasons)


def _final_dispatch_status(
    *,
    noop_reason: str | None,
    sensitive_flags: list[str],
    preflight_summary_status: str,
    block_reasons: list[str],
    approval_reasons: list[str],
    channels: list[dict[str, Any]],
) -> str:
    if noop_reason:
        return DISPATCH_NOOP
    if sensitive_flags or preflight_summary_status == PREFLIGHT_SAFETY_BLOCKED:
        return DISPATCH_SAFETY_BLOCKED
    if preflight_summary_status == PREFLIGHT_BLOCKED or block_reasons:
        return DISPATCH_BLOCKED
    if preflight_summary_status == PREFLIGHT_NEEDS_APPROVAL or approval_reasons:
        return DISPATCH_NEEDS_APPROVAL
    if any(channel.get("enabled") is True for channel in channels):
        return DISPATCH_WOULD_SEND
    return DISPATCH_BLOCKED


def _subject_preview(*, draft_payload: dict[str, Any], email_body: str, as_of: date) -> str:
    subject = _string_value(
        _mapping(_mapping(draft_payload.get("draft_outputs")).get("email_draft")).get("subject")
    )
    if subject:
        return subject
    for line in email_body.splitlines():
        if line.lower().startswith("subject:"):
            return line.split(":", 1)[1].strip()
    return f"[Trading System] Daily Operator Brief - {as_of.isoformat()}"


def _title_preview(
    *,
    operator_payload: dict[str, Any],
    operator_markdown: str,
    as_of: date,
) -> str:
    for line in operator_markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped.removeprefix("# ").strip()
    headline = _string_value(operator_payload.get("headline"))
    return headline or f"Daily Trading System Operator Brief - {as_of.isoformat()}"


def _operator_brief_json_body(operator_payload: dict[str, Any]) -> str:
    parts = [
        _string_value(operator_payload.get("headline")),
        *_strings(operator_payload.get("recommended_next_steps")),
    ]
    alerts = _mapping(operator_payload.get("alerts"))
    parts.extend(_strings(alerts.get("critical")))
    parts.extend(_strings(alerts.get("warnings")))
    return "\n".join(part for part in parts if part)


def _body_excerpt(value: str) -> str:
    normalized = value.strip()
    if len(normalized) <= BODY_EXCERPT_MAX_CHARS:
        return normalized
    return normalized[: BODY_EXCERPT_MAX_CHARS - 3].rstrip() + "..."


def _contains_markdown(value: str) -> bool:
    return any(token in value for token in ("# ", "## ", "- ", "|---", "> "))


def _sensitive_content_flags(value: str, *, label: str) -> list[str]:
    flags: list[str] = []
    for match in SENSITIVE_ASSIGNMENT_RE.finditer(value):
        field = match.group(1)
        raw_value = match.group(3).strip().strip("\"'")
        if _is_redacted_value(raw_value):
            continue
        flags.append(f"{label} contains unredacted sensitive field `{field}`.")
    if PRIVATE_KEY_RE.search(value):
        flags.append(f"{label} contains private key material.")
    return _unique_strings(flags)


def _url_safety_flags(payload: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    alerts = _mapping(payload.get("alerts"))
    for warning in [*_strings(alerts.get("warnings")), *_strings(alerts.get("critical"))]:
        if "webhook url" in warning.lower() or URL_RE.search(warning):
            flags.append("Preflight reported a webhook or external endpoint; dry-run blocked it.")
    return _unique_strings(flags)


def _redact_sensitive_text(value: str) -> str:
    redacted = SENSITIVE_ASSIGNMENT_RE.sub(lambda match: f"{match.group(1)}=[REDACTED]", value)
    redacted = PRIVATE_KEY_RE.sub("[REDACTED_PRIVATE_KEY]", redacted)
    return redacted


def _mask_recipient(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        email = match.group(0)
        local, domain = email.split("@", 1)
        prefix = local[:1] if local else "x"
        return f"{prefix}***@{domain}"

    return EMAIL_RE.sub(replace, value)


def _is_redacted_value(value: str) -> bool:
    normalized = value.strip().strip("\"'").upper()
    return normalized in {"[REDACTED]", "REDACTED"} or normalized.startswith("[REDACTED]")


def _artifact_path_safety_findings(
    *,
    payloads: tuple[dict[str, Any], ...],
    project_root: Path,
    data_root: Path,
) -> list[str]:
    findings: list[str] = []
    for payload in payloads:
        for raw_path in _path_values(payload):
            _path, finding = _resolve_repo_path(
                value=Path(raw_path),
                data_root=data_root,
                project_root=project_root,
                purpose="artifact declared path",
            )
            if finding:
                findings.append(finding)
    return _unique_strings(findings)


def _path_values(value: object) -> list[str]:
    values: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "path" and isinstance(child, str):
                values.append(child)
            else:
                values.extend(_path_values(child))
    elif isinstance(value, list):
        for child in value:
            values.extend(_path_values(child))
    return values


def _template_refs(
    *,
    draft_payload: dict[str, Any],
    draft_texts: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    if draft_payload:
        refs.append(
            {
                "template_type": "notification_draft_metadata",
                "task_id": draft_payload.get("task_id"),
                "status": draft_payload.get("draft_status"),
            }
        )
    for key, record in draft_texts.items():
        refs.append(
            {
                "template_type": key,
                "path": record.get("path"),
                "status": record.get("status"),
                "sha256": record.get("sha256"),
            }
        )
    return refs


def _input_ref(path: Path, status: str) -> dict[str, Any]:
    return {
        "path": str(path),
        "status": status,
        "sha256": _sha256_path(path) if status == STATUS_FOUND and path.is_file() else None,
    }


def _path_from_ref(value: object) -> str:
    return _string_value(_mapping(value).get("path"))


def _next_recommended_action(*, final_status: str, noop_reason: str | None) -> str:
    if final_status == DISPATCH_WOULD_SEND:
        return "Manual reviewer may approve a future real dispatch task; this run sent nothing."
    if final_status == DISPATCH_NEEDS_APPROVAL:
        return "Obtain explicit human approval before any future real dispatch task."
    if final_status == DISPATCH_SAFETY_BLOCKED:
        return "Resolve safety findings before reviewing notification dispatch."
    if final_status == DISPATCH_NOOP:
        return noop_reason or "No notification dispatch is needed for this run."
    return "Restore required artifacts or fix preflight blockers before dispatch review."


def _normalize_data_root(data_root: Path) -> Path:
    value = Path(data_root)
    if value.is_absolute():
        return value.resolve(strict=False)
    return (Path.cwd() / value).resolve(strict=False)


def _project_root_from_data_root(data_root: Path) -> Path:
    return data_root.parent if data_root.name == "data" else REPO_ROOT


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _string_value(value: object) -> str:
    return value if isinstance(value, str) else ""


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            unique.append(value)
    return unique


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _bool_text(value: object) -> str:
    return "true" if value is True else "false"


def _markdown_list(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _assert_dispatch_preview_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-032 production_effect must be none")
    for field in SAFETY_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-032 {field} must be true")
    for field in SAFETY_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-032 {field} must be false")
    serialized = json.dumps(payload, ensure_ascii=False)
    if "sent_at" in serialized:
        raise ValueError("TRADING-032 must not emit sent_at")
