from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "operator_brief_notification_delivery_preflight"
RUN_REPORT_TYPE = "operator_brief_notification_delivery_preflight_run"
TASK_ID = "TRADING-031"
INPUT_TASK_ID = "TRADING-030"
MODE = "operator_brief_notification_delivery_preflight_only"
PRODUCTION_EFFECT_NONE = "none"

PREFLIGHT_PASS = "PASS"
PREFLIGHT_PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"
PREFLIGHT_BLOCKED = "BLOCKED"
PREFLIGHT_INPUT_MISSING = "INPUT_MISSING"
PREFLIGHT_INPUT_INVALID = "INPUT_INVALID"
PREFLIGHT_SAFETY_BLOCKED = "SAFETY_BLOCKED"
PREFLIGHT_ERROR = "ERROR"

READINESS_READY = "READY_FOR_MANUAL_REVIEW"
READINESS_NEEDS_APPROVAL = "NEEDS_APPROVAL"
READINESS_BLOCKED = "BLOCKED"
READINESS_SAFETY_BLOCKED = "SAFETY_BLOCKED"
READINESS_UNKNOWN = "UNKNOWN"

SEVERITY_NORMAL = "NORMAL"
SEVERITY_WATCH = "WATCH"
SEVERITY_ACTION = "ACTION"
SEVERITY_URGENT = "URGENT"
SEVERITY_BLOCKED = "BLOCKED"
SEVERITY_UNKNOWN = "UNKNOWN"

STATUS_FOUND = "FOUND"
STATUS_MISSING = "MISSING"
STATUS_INVALID = "INVALID"
STATUS_EMPTY = "EMPTY"
STATUS_OPTIONAL_FOUND = "OPTIONAL_FOUND"
STATUS_OPTIONAL_NOT_FOUND = "OPTIONAL_NOT_FOUND"
STATUS_OPTIONAL_INVALID = "OPTIONAL_INVALID"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

RECIPIENT_CONFIG_FILENAME = "notification_recipients.example.json"
CHANNEL_CONFIG_FILENAME = "notification_channels.example.json"
APPROVAL_CONFIG_FILENAME = "notification_approval_policy.example.json"

OUTPUT_SAFETY_TRUE_FIELDS = (
    "manual_review_only",
    "notification_delivery_preflight_only",
    "read_only",
    "safe_for_scheduler",
)
OUTPUT_SAFETY_FALSE_FIELDS = (
    "email_sent",
    "gmail_draft_created",
    "gmail_draft_modified",
    "slack_sent",
    "discord_sent",
    "webhook_called",
    "mobile_push_sent",
    "operator_brief_executed_by_delivery_preflight",
    "notification_draft_executed_by_delivery_preflight",
    "pipelines_executed_by_delivery_preflight",
    "data_downloaded_by_delivery_preflight",
    "apply_executed_by_delivery_preflight",
    "rollback_executed_by_delivery_preflight",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)
INPUT_METADATA_TRUE_FIELDS = (
    "notification_draft_only",
    "read_only",
)
INPUT_METADATA_FALSE_FIELDS = (
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
SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"\b(api_key|apiKey|secret|token|password|credential|broker_token|account_id|"
    r"private_key)\b\s*([:=])\s*(\"[^\"]*\"|'[^']*'|\[[^\]]+\]|[^\s,;`]+)",
    re.IGNORECASE,
)


def default_notification_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs" / "notifications"


def default_delivery_preflight_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "delivery_preflight"


def default_notification_metadata_path(data_root: Path, as_of: date) -> Path:
    return default_notification_root(data_root) / (
        f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    )


def default_delivery_preflight_json_path(data_root: Path, as_of: date) -> Path:
    return default_delivery_preflight_root(data_root) / (
        f"operator_brief_notification_delivery_preflight_{as_of.isoformat()}.json"
    )


def default_delivery_preflight_markdown_path(data_root: Path, as_of: date) -> Path:
    return default_delivery_preflight_json_path(data_root, as_of).with_suffix(".md")


def default_delivery_preflight_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_delivery_preflight_root(data_root)
        / "logs"
        / f"operator_brief_notification_delivery_preflight_run_{as_of.isoformat()}.json"
    )


def write_operator_brief_notification_delivery_preflight(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    notification_draft_metadata_file: Path | None = None,
    recipient_config_file: Path | None = None,
    channel_config_file: Path | None = None,
    approval_config_file: Path | None = None,
    allow_missing_recipient_config: bool = False,
    allow_missing_channel_config: bool = False,
    fail_on_urgent_without_approval: bool = False,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = Path(data_root)
    json_path = default_delivery_preflight_json_path(data_root, as_of)
    markdown_path = json_path.with_suffix(".md")
    run_log_json_path = default_delivery_preflight_run_log_json_path(data_root, as_of)
    run_log_markdown_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_operator_brief_notification_delivery_preflight(
            as_of=as_of,
            data_root=data_root,
            notification_draft_metadata_file=notification_draft_metadata_file,
            recipient_config_file=recipient_config_file,
            channel_config_file=channel_config_file,
            approval_config_file=approval_config_file,
            allow_missing_recipient_config=allow_missing_recipient_config,
            allow_missing_channel_config=allow_missing_channel_config,
            fail_on_urgent_without_approval=fail_on_urgent_without_approval,
            output_json_path=json_path,
            output_markdown_path=markdown_path,
            run_log_json_path=run_log_json_path,
            run_log_markdown_path=run_log_markdown_path,
            generated_at=generated,
        )
    except Exception as exc:  # pragma: no cover - defensive artifact path
        payload = _error_payload(
            as_of=as_of,
            data_root=data_root,
            output_json_path=json_path,
            output_markdown_path=markdown_path,
            run_log_json_path=run_log_json_path,
            run_log_markdown_path=run_log_markdown_path,
            generated_at=generated,
            error=str(exc),
        )

    write_json_atomic(json_path, payload, sort_keys=False)
    write_text_atomic(markdown_path, render_delivery_preflight_markdown(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    write_json_atomic(run_log_json_path, run_log, sort_keys=False)
    write_text_atomic(run_log_markdown_path, render_delivery_preflight_run_log(run_log))
    return payload


def build_operator_brief_notification_delivery_preflight(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    notification_draft_metadata_file: Path | None = None,
    recipient_config_file: Path | None = None,
    channel_config_file: Path | None = None,
    approval_config_file: Path | None = None,
    allow_missing_recipient_config: bool = False,
    allow_missing_channel_config: bool = False,
    fail_on_urgent_without_approval: bool = False,
    output_json_path: Path | None = None,
    output_markdown_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_markdown_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = Path(data_root)
    output_json_path = output_json_path or default_delivery_preflight_json_path(data_root, as_of)
    output_markdown_path = output_markdown_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_delivery_preflight_run_log_json_path(
        data_root, as_of
    )
    run_log_markdown_path = run_log_markdown_path or run_log_json_path.with_suffix(".md")

    metadata_path = _resolve_notification_metadata_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=notification_draft_metadata_file,
    )
    metadata_payload, metadata_status, metadata_error = _read_json_object_with_status(metadata_path)
    metadata_task_id_valid = (
        metadata_status == STATUS_FOUND and metadata_payload.get("task_id") == INPUT_TASK_ID
    )
    metadata_usable = metadata_status == STATUS_FOUND and metadata_task_id_valid
    severity = (
        _string_value(metadata_payload.get("notification_severity")) or SEVERITY_UNKNOWN
        if metadata_usable
        else SEVERITY_UNKNOWN
    )
    metadata_safety = _notification_metadata_safety_validation(
        payload=metadata_payload,
        status=metadata_status,
        task_id_valid=metadata_task_id_valid,
    )
    drafts = _load_draft_artifacts(
        metadata_payload=metadata_payload if metadata_usable else {},
        data_root=data_root,
    )
    configs = _load_optional_configs(
        data_root=data_root,
        recipient_config_file=recipient_config_file,
        channel_config_file=channel_config_file,
        approval_config_file=approval_config_file,
    )
    config_safety = _config_safety_findings(configs)
    draft_validation = _draft_validation(
        drafts=drafts,
        metadata_safe=metadata_safety["status"] == "PASS",
    )
    approval_validation = _approval_validation(
        severity=severity,
        approval_config=configs["approval_config"],
        config_safety_blocked=bool(config_safety["blocking_reasons"]),
        fail_on_urgent_without_approval=fail_on_urgent_without_approval,
    )
    channel_readiness = _channel_readiness(
        drafts=drafts,
        recipient_config=configs["recipient_config"],
        channel_config=configs["channel_config"],
        blocked_by_safety=bool(config_safety["blocking_reasons"]),
    )
    safety_validation = _safety_validation(
        metadata_task_id_valid=metadata_task_id_valid,
        metadata_safe=metadata_safety["status"] == "PASS",
        metadata_blocking_reasons=metadata_safety["blocking_reasons"],
        config_blocking_reasons=config_safety["blocking_reasons"],
    )
    preflight_status = _preflight_status(
        metadata_status=metadata_status,
        metadata_task_id_valid=metadata_task_id_valid,
        metadata_safety=safety_validation,
        draft_validation=draft_validation,
        approval_validation=approval_validation,
        severity=severity,
    )
    delivery_readiness = _delivery_readiness(
        preflight_status=preflight_status,
        severity=severity,
        approval_required=approval_validation["approval_required"] is True,
    )
    alerts = _alerts(
        preflight_status=preflight_status,
        metadata_error=metadata_error,
        metadata_safety=metadata_safety,
        draft_validation=draft_validation,
        approval_validation=approval_validation,
        channel_readiness=channel_readiness,
        config_safety=config_safety,
        allow_missing_recipient_config=allow_missing_recipient_config,
        allow_missing_channel_config=allow_missing_channel_config,
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_delivery_preflight_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_delivery_preflight": False,
        "notification_draft_executed_by_delivery_preflight": False,
        "pipelines_executed_by_delivery_preflight": False,
        "data_downloaded_by_delivery_preflight": False,
        "apply_executed_by_delivery_preflight": False,
        "rollback_executed_by_delivery_preflight": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "preflight_status": preflight_status,
        "delivery_readiness": delivery_readiness,
        "notification_severity": severity,
        "headline": _headline(preflight_status, delivery_readiness),
        "input_artifacts": {
            "notification_draft_metadata": _artifact_record(metadata_path, metadata_status),
            "email_draft": _draft_artifact_record(drafts["email_draft"]),
            "chat_draft": _draft_artifact_record(drafts["chat_draft"]),
            "mobile_summary": _draft_artifact_record(drafts["mobile_summary"]),
            "recipient_config": configs["recipient_config"]["artifact"],
            "channel_config": configs["channel_config"]["artifact"],
            "approval_config": configs["approval_config"]["artifact"],
        },
        "draft_validation": draft_validation,
        "approval_validation": approval_validation,
        "channel_readiness": channel_readiness,
        "safety_validation": safety_validation,
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(
            preflight_status=preflight_status,
            delivery_readiness=delivery_readiness,
        ),
        "manual_review_required": {
            "required": True,
            "instructions": [
                "Review all draft content before sending.",
                "Confirm redaction is correct.",
                "Confirm recipient/channel manually.",
                "Do not send automatically from TRADING-031.",
            ],
        },
        "audit": {
            "created_by": "scripts/run_operator_brief_notification_delivery_preflight.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_delivery_preflight_artifacts": True,
        },
        "output_artifacts": {
            "preflight_json": {"path": str(output_json_path)},
            "preflight_markdown": {"path": str(output_markdown_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_markdown_path)},
        },
    }
    _assert_delivery_preflight_safety_invariants(payload)
    return payload


def render_delivery_preflight_markdown(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    drafts = _mapping(payload.get("draft_validation"))
    approval = _mapping(payload.get("approval_validation"))
    channel_readiness = _mapping(payload.get("channel_readiness"))
    safety = _mapping(payload.get("safety_validation"))
    alerts = _mapping(payload.get("alerts"))
    lines = [
        f"# Operator Brief Notification Delivery Preflight - {payload.get('date')}",
        "",
    ]
    if payload.get("preflight_status") == PREFLIGHT_SAFETY_BLOCKED:
        lines.extend(["## Notification Delivery Preflight Safety Blocked", ""])
    if payload.get("delivery_readiness") == READINESS_NEEDS_APPROVAL:
        lines.extend(["## Notification Delivery Needs Approval", ""])
    if payload.get("preflight_status") == PREFLIGHT_BLOCKED:
        lines.extend(["## Notification Delivery Blocked", ""])
    lines.extend(
        [
            "## 1. Preflight Summary",
            "",
            f"- Preflight Status: `{payload.get('preflight_status')}`",
            f"- Delivery Readiness: `{payload.get('delivery_readiness')}`",
            f"- Notification Severity: `{payload.get('notification_severity')}`",
            f"- Manual Review Only: `{_bool_text(payload.get('manual_review_only'))}`",
            f"- Email Sent: `{_bool_text(payload.get('email_sent'))}`",
            f"- Gmail Draft Created: `{_bool_text(payload.get('gmail_draft_created'))}`",
            f"- Slack Sent: `{_bool_text(payload.get('slack_sent'))}`",
            f"- Discord Sent: `{_bool_text(payload.get('discord_sent'))}`",
            f"- Mobile Push Sent: `{_bool_text(payload.get('mobile_push_sent'))}`",
            "",
            "## 2. Draft Validation",
            "",
            "| Draft | Status | Path | Notes |",
            "|---|---:|---|---|",
            _draft_row("Email", artifacts.get("email_draft"), "ready for manual review"),
            _draft_row("Chat", artifacts.get("chat_draft"), "ready for manual review"),
            _draft_row("Mobile", artifacts.get("mobile_summary"), "ready for manual review"),
            "",
            f"- Draft Validation Status: `{drafts.get('status')}`",
            f"- Redaction Confirmed: `{_bool_text(drafts.get('redaction_confirmed'))}`",
            "- Sensitive Content Detected: "
            f"`{_bool_text(drafts.get('sensitive_content_detected'))}`",
            "",
            "## 3. Approval Validation",
            "",
            f"- Approval Required: `{_bool_text(approval.get('approval_required'))}`",
            "- Approval Policy Available: "
            f"`{_bool_text(approval.get('approval_policy_available'))}`",
            f"- Approval Reason: {approval.get('approval_reason') or ''}",
            "",
            "## 4. Channel Readiness",
            "",
            "| Channel | Status | Manual Send Only | Blocking Reasons | Warnings |",
            "|---|---:|---:|---|---|",
        ]
    )
    for channel in ("email", "chat", "mobile"):
        lines.append(_channel_row(channel.title(), _mapping(channel_readiness.get(channel))))
    lines.extend(
        [
            "",
            "## 5. Safety Validation",
            "",
            "| Check | Status |",
            "|---|---:|",
            f"| No Email Sent | `{_pass_fail(payload.get('email_sent') is False)}` |",
            "| No Gmail Draft Created | "
            f"`{_pass_fail(payload.get('gmail_draft_created') is False)}` |",
            f"| No Webhook Called | `{_pass_fail(payload.get('webhook_called') is False)}` |",
            "| No Mobile Push Sent | "
            f"`{_pass_fail(payload.get('mobile_push_sent') is False)}` |",
            "| No Broker Execution | "
            f"`{_pass_fail(payload.get('broker_execution') is False)}` |",
            "| No Replay Execution | "
            f"`{_pass_fail(payload.get('replay_execution') is False)}` |",
            "| No Trading Execution | "
            f"`{_pass_fail(payload.get('trading_execution') is False)}` |",
            f"| Metadata Safe | `{_pass_fail(safety.get('notification_metadata_safe'))}` |",
            "",
            "## 6. Alerts",
            "",
            "### Critical",
            "",
            *_markdown_list(_strings(alerts.get("critical"))),
            "",
            "### Warnings",
            "",
            *_markdown_list(_strings(alerts.get("warnings"))),
            "",
            "### Notes",
            "",
            *_markdown_list(_strings(alerts.get("notes"))),
            "",
            "## 7. Recommended Next Steps",
            "",
            *_markdown_list(_strings(payload.get("recommended_next_steps"))),
            "",
        ]
    )
    return "\n".join(lines)


def render_delivery_preflight_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Operator Brief Notification Delivery Preflight Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- preflight_status: `{payload.get('preflight_status')}`",
            f"- delivery_readiness: `{payload.get('delivery_readiness')}`",
            f"- notification_severity: `{payload.get('notification_severity')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- notification_delivery_preflight_only: `true`",
            "- read_only: `true`",
            "- email_sent: `false`",
            "- gmail_draft_created: `false`",
            "- gmail_draft_modified: `false`",
            "- slack_sent: `false`",
            "- discord_sent: `false`",
            "- webhook_called: `false`",
            "- mobile_push_sent: `false`",
            "- operator_brief_executed_by_delivery_preflight: `false`",
            "- notification_draft_executed_by_delivery_preflight: `false`",
            "- pipelines_executed_by_delivery_preflight: `false`",
            "- data_downloaded_by_delivery_preflight: `false`",
            "- apply_executed_by_delivery_preflight: `false`",
            "- rollback_executed_by_delivery_preflight: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- preflight_json: `{payload.get('preflight_json')}`",
            f"- preflight_markdown: `{payload.get('preflight_markdown')}`",
            "",
        ]
    )


def should_fail_cli(payload: dict[str, Any]) -> bool:
    return payload.get("preflight_status") in {
        PREFLIGHT_BLOCKED,
        PREFLIGHT_INPUT_MISSING,
        PREFLIGHT_INPUT_INVALID,
        PREFLIGHT_SAFETY_BLOCKED,
        PREFLIGHT_ERROR,
    }


def _resolve_notification_metadata_path(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
) -> Path:
    if explicit_path is not None:
        return _resolve_input_path(data_root, explicit_path)
    notification_root = default_notification_root(data_root)
    default_path = default_notification_metadata_path(data_root, as_of)
    if not notification_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in notification_root.glob("operator_brief_notification_draft_*.json"):
        raw_date = path.stem.removeprefix("operator_brief_notification_draft_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _load_draft_artifacts(
    *,
    metadata_payload: dict[str, Any],
    data_root: Path,
) -> dict[str, dict[str, Any]]:
    outputs = _mapping(metadata_payload.get("draft_outputs"))
    return {
        "email_draft": _load_draft_artifact(
            label="email_draft",
            path_value=_mapping(outputs.get("email_draft")).get("path"),
            data_root=data_root,
        ),
        "chat_draft": _load_draft_artifact(
            label="chat_draft",
            path_value=_mapping(outputs.get("chat_draft")).get("path"),
            data_root=data_root,
        ),
        "mobile_summary": _load_draft_artifact(
            label="mobile_summary",
            path_value=_mapping(outputs.get("mobile_summary")).get("path"),
            data_root=data_root,
        ),
    }


def _load_draft_artifact(*, label: str, path_value: object, data_root: Path) -> dict[str, Any]:
    path_text = _string_value(path_value)
    if not path_text:
        return {
            "label": label,
            "status": STATUS_MISSING,
            "path": None,
            "sha256": None,
            "content": "",
            "non_empty": False,
            "blocking_reasons": [f"{label} path is missing from TRADING-030 metadata."],
            "sensitive_findings": [],
        }
    path = _resolve_input_path(data_root, Path(path_text))
    if not path.exists():
        return {
            "label": label,
            "status": STATUS_MISSING,
            "path": str(path),
            "sha256": None,
            "content": "",
            "non_empty": False,
            "blocking_reasons": [f"{label} file is missing."],
            "sensitive_findings": [],
        }
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        return {
            "label": label,
            "status": STATUS_INVALID,
            "path": str(path),
            "sha256": None,
            "content": "",
            "non_empty": False,
            "blocking_reasons": [f"{label} file is not readable: {exc}"],
            "sensitive_findings": [],
        }
    non_empty = bool(content.strip())
    sensitive_findings = _sensitive_findings(content, label=label)
    return {
        "label": label,
        "status": STATUS_FOUND if non_empty else STATUS_EMPTY,
        "path": str(path),
        "sha256": _sha256_path(path),
        "content": content,
        "non_empty": non_empty,
        "blocking_reasons": [] if non_empty else [f"{label} file is empty."],
        "sensitive_findings": sensitive_findings,
    }


def _load_optional_configs(
    *,
    data_root: Path,
    recipient_config_file: Path | None,
    channel_config_file: Path | None,
    approval_config_file: Path | None,
) -> dict[str, dict[str, Any]]:
    return {
        "recipient_config": _load_optional_json_config(
            data_root=data_root,
            explicit_path=recipient_config_file,
            filename=RECIPIENT_CONFIG_FILENAME,
        ),
        "channel_config": _load_optional_json_config(
            data_root=data_root,
            explicit_path=channel_config_file,
            filename=CHANNEL_CONFIG_FILENAME,
        ),
        "approval_config": _load_optional_json_config(
            data_root=data_root,
            explicit_path=approval_config_file,
            filename=APPROVAL_CONFIG_FILENAME,
        ),
    }


def _load_optional_json_config(
    *,
    data_root: Path,
    explicit_path: Path | None,
    filename: str,
) -> dict[str, Any]:
    path = (
        _resolve_input_path(data_root, explicit_path)
        if explicit_path is not None
        else data_root.parent / "config" / filename
    )
    payload, status, error = _read_json_object_with_status(path)
    artifact_status = {
        STATUS_FOUND: STATUS_OPTIONAL_FOUND,
        STATUS_MISSING: STATUS_OPTIONAL_NOT_FOUND,
        STATUS_INVALID: STATUS_OPTIONAL_INVALID,
    }[status]
    return {
        "payload": payload,
        "status": artifact_status,
        "error": error,
        "artifact": _artifact_record(path, artifact_status),
    }


def _notification_metadata_safety_validation(
    *,
    payload: dict[str, Any],
    status: str,
    task_id_valid: bool,
) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    if status != STATUS_FOUND:
        return {
            "status": "PASS",
            "notification_metadata_task_id_valid": False,
            "notification_metadata_safe": False,
            "blocking_reasons": blocking_reasons,
        }
    if not task_id_valid:
        blocking_reasons.append("TRADING-030 metadata task_id is invalid.")
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        blocking_reasons.append("TRADING-030 metadata production_effect must be none.")
    if payload.get("manual_review_only") is not True:
        blocking_reasons.append("TRADING-030 metadata manual_review_only must be true.")
    for field in INPUT_METADATA_TRUE_FIELDS:
        if payload.get(field) is not True:
            blocking_reasons.append(f"TRADING-030 metadata {field} must be true.")
    for field in INPUT_METADATA_FALSE_FIELDS:
        if payload.get(field) is not False:
            blocking_reasons.append(f"TRADING-030 metadata {field} must be false.")
    return {
        "status": "FAIL" if blocking_reasons else "PASS",
        "notification_metadata_task_id_valid": task_id_valid,
        "notification_metadata_safe": not blocking_reasons,
        "blocking_reasons": blocking_reasons,
    }


def _draft_validation(
    *,
    drafts: dict[str, dict[str, Any]],
    metadata_safe: bool,
) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    sensitive_findings: list[str] = []
    for draft in drafts.values():
        blocking_reasons.extend(_strings(draft.get("blocking_reasons")))
        sensitive_findings.extend(_strings(draft.get("sensitive_findings")))
    sensitive_content_detected = bool(sensitive_findings)
    status = "PASS"
    if sensitive_content_detected:
        status = PREFLIGHT_SAFETY_BLOCKED
    elif blocking_reasons:
        status = PREFLIGHT_BLOCKED
    return {
        "status": status,
        "metadata_safe": metadata_safe,
        "email_draft_available": drafts["email_draft"]["status"] == STATUS_FOUND,
        "chat_draft_available": drafts["chat_draft"]["status"] == STATUS_FOUND,
        "mobile_summary_available": drafts["mobile_summary"]["status"] == STATUS_FOUND,
        "email_draft_non_empty": drafts["email_draft"].get("non_empty") is True,
        "chat_draft_non_empty": drafts["chat_draft"].get("non_empty") is True,
        "mobile_summary_non_empty": drafts["mobile_summary"].get("non_empty") is True,
        "redaction_confirmed": not sensitive_content_detected,
        "sensitive_content_detected": sensitive_content_detected,
        "blocking_reasons": blocking_reasons,
        "warnings": [],
        "sensitive_findings": sensitive_findings,
    }


def _approval_validation(
    *,
    severity: str,
    approval_config: dict[str, Any],
    config_safety_blocked: bool,
    fail_on_urgent_without_approval: bool,
) -> dict[str, Any]:
    payload = _mapping(approval_config.get("payload"))
    policy_available = approval_config.get("status") == STATUS_OPTIONAL_FOUND
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    require_for = set(_strings(payload.get("require_approval_for"))) if policy_available else set()
    allow_manual = (
        set(_strings(payload.get("allow_manual_review_for"))) if policy_available else set()
    )
    if policy_available and severity in allow_manual:
        approval_required = False
        reason = f"{severity} severity is allowed for manual review by approval policy."
    elif policy_available:
        approval_required = severity in require_for or severity in {
            SEVERITY_ACTION,
            SEVERITY_URGENT,
            SEVERITY_BLOCKED,
        }
        reason = (
            f"{severity} severity requires delivery approval by policy."
            if approval_required
            else f"{severity} severity does not require delivery approval by policy."
        )
    else:
        approval_required = severity in {SEVERITY_ACTION, SEVERITY_URGENT, SEVERITY_BLOCKED}
        reason = (
            f"{severity} severity requires approval, but approval policy was not found."
            if approval_required
            else f"{severity} severity does not require delivery approval."
        )
    if not policy_available and approval_required:
        warnings.append("Approval policy was not found for a severity that requires approval.")
    if fail_on_urgent_without_approval and severity == SEVERITY_URGENT and not policy_available:
        blocking_reasons.append("URGENT severity requires approval policy in strict mode.")
    if severity == SEVERITY_BLOCKED:
        blocking_reasons.append("BLOCKED notification severity cannot enter delivery.")
    status = "PASS"
    if config_safety_blocked:
        status = PREFLIGHT_SAFETY_BLOCKED
    elif blocking_reasons:
        status = PREFLIGHT_BLOCKED
    elif warnings:
        status = PREFLIGHT_PASS_WITH_WARNINGS
    return {
        "status": status,
        "approval_required": approval_required,
        "approval_policy_available": policy_available,
        "approval_reason": reason,
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
    }


def _channel_readiness(
    *,
    drafts: dict[str, dict[str, Any]],
    recipient_config: dict[str, Any],
    channel_config: dict[str, Any],
    blocked_by_safety: bool,
) -> dict[str, Any]:
    channel_payload = _mapping(channel_config.get("payload"))
    recipient_available = recipient_config.get("status") == STATUS_OPTIONAL_FOUND
    channel_available = channel_config.get("status") == STATUS_OPTIONAL_FOUND
    return {
        "email": _single_channel_readiness(
            channel="email",
            draft=drafts["email_draft"],
            recipient_config_available=recipient_available,
            channel_config_available=channel_available,
            channel_payload=channel_payload,
            blocked_by_safety=blocked_by_safety,
        ),
        "chat": _single_channel_readiness(
            channel="chat",
            draft=drafts["chat_draft"],
            recipient_config_available=None,
            channel_config_available=channel_available,
            channel_payload=channel_payload,
            blocked_by_safety=blocked_by_safety,
        ),
        "mobile": _single_channel_readiness(
            channel="mobile",
            draft=drafts["mobile_summary"],
            recipient_config_available=None,
            channel_config_available=channel_available,
            channel_payload=channel_payload,
            blocked_by_safety=blocked_by_safety,
        ),
    }


def _single_channel_readiness(
    *,
    channel: str,
    draft: dict[str, Any],
    recipient_config_available: bool | None,
    channel_config_available: bool,
    channel_payload: dict[str, Any],
    blocked_by_safety: bool,
) -> dict[str, Any]:
    blocking_reasons = list(_strings(draft.get("blocking_reasons")))
    warnings: list[str] = []
    channel_settings = _mapping(channel_payload.get(channel))
    if blocked_by_safety:
        blocking_reasons.append("Channel config or approval config violates read-only policy.")
    if channel_settings and channel_settings.get("enabled") is False:
        blocking_reasons.append(f"{channel} channel is disabled in channel config.")
    if channel == "email" and recipient_config_available is False:
        warnings.append("Recipient config was not found. Email can only be reviewed manually.")
    if channel == "chat" and not channel_config_available:
        warnings.append("Channel config was not found. Chat draft can only be copied manually.")
    if channel == "mobile" and not channel_config_available:
        warnings.append("Mobile push config was not found. Mobile summary is review-only.")
    status = PREFLIGHT_BLOCKED if blocking_reasons else READINESS_READY
    record: dict[str, Any] = {
        "status": status,
        "draft_available": draft.get("status") == STATUS_FOUND,
        "can_send_automatically": False,
        "manual_send_only": True,
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
    }
    if channel == "email":
        record["recipient_config_available"] = recipient_config_available is True
    else:
        record["channel_config_available"] = channel_config_available
    return record


def _config_safety_findings(configs: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    blocking_reasons: list[str] = []
    warnings: list[str] = []
    for config_name in ("channel_config", "approval_config"):
        config = configs[config_name]
        if config.get("status") != STATUS_OPTIONAL_FOUND:
            continue
        payload = _mapping(config.get("payload"))
        for path, key, value in _walk_config(payload):
            lowered_key = key.lower()
            if (
                lowered_key
                in {
                    "auto_send_allowed",
                    "gmail_draft_creation_allowed",
                    "webhook_send_allowed",
                    "mobile_push_allowed",
                }
                and value is True
            ):
                blocking_reasons.append(f"{config_name} {path} enables {key}=true.")
            if lowered_key == "manual_send_only" and value is not True:
                blocking_reasons.append(f"{config_name} {path} must keep manual_send_only=true.")
            if "webhook" in lowered_key and isinstance(value, str) and value.strip():
                warnings.append("Webhook URL was present in config and was ignored/redacted.")
    return {
        "blocking_reasons": _unique_strings(blocking_reasons),
        "warnings": _unique_strings(warnings),
    }


def _safety_validation(
    *,
    metadata_task_id_valid: bool,
    metadata_safe: bool,
    metadata_blocking_reasons: list[str],
    config_blocking_reasons: list[str],
) -> dict[str, Any]:
    blocking_reasons = [*metadata_blocking_reasons, *config_blocking_reasons]
    return {
        "status": "FAIL" if blocking_reasons else "PASS",
        "notification_metadata_task_id_valid": metadata_task_id_valid,
        "notification_metadata_safe": metadata_safe,
        "no_email_sent": True,
        "no_gmail_draft_created": True,
        "no_webhook_called": True,
        "no_mobile_push_sent": True,
        "no_pipeline_execution": True,
        "no_data_download": True,
        "no_apply_or_rollback": True,
        "no_broker_replay_trading": True,
        "blocking_reasons": _unique_strings(blocking_reasons),
    }


def _preflight_status(
    *,
    metadata_status: str,
    metadata_task_id_valid: bool,
    metadata_safety: dict[str, Any],
    draft_validation: dict[str, Any],
    approval_validation: dict[str, Any],
    severity: str,
) -> str:
    if metadata_status == STATUS_MISSING:
        return PREFLIGHT_INPUT_MISSING
    if metadata_status == STATUS_INVALID or not metadata_task_id_valid:
        return PREFLIGHT_INPUT_INVALID
    if metadata_safety.get("status") == "FAIL":
        return PREFLIGHT_SAFETY_BLOCKED
    if draft_validation.get("sensitive_content_detected") is True:
        return PREFLIGHT_SAFETY_BLOCKED
    if draft_validation.get("blocking_reasons"):
        return PREFLIGHT_BLOCKED
    if severity == SEVERITY_BLOCKED:
        return PREFLIGHT_BLOCKED
    if approval_validation.get("status") == PREFLIGHT_SAFETY_BLOCKED:
        return PREFLIGHT_SAFETY_BLOCKED
    if approval_validation.get("status") == PREFLIGHT_BLOCKED:
        return PREFLIGHT_BLOCKED
    if approval_validation.get("status") == PREFLIGHT_PASS_WITH_WARNINGS:
        return PREFLIGHT_PASS_WITH_WARNINGS
    return PREFLIGHT_PASS


def _delivery_readiness(
    *,
    preflight_status: str,
    severity: str,
    approval_required: bool,
) -> str:
    if preflight_status == PREFLIGHT_SAFETY_BLOCKED:
        return READINESS_SAFETY_BLOCKED
    if preflight_status == PREFLIGHT_BLOCKED:
        return READINESS_BLOCKED
    if preflight_status in {PREFLIGHT_INPUT_MISSING, PREFLIGHT_INPUT_INVALID, PREFLIGHT_ERROR}:
        return READINESS_UNKNOWN
    if approval_required or severity in {SEVERITY_ACTION, SEVERITY_URGENT}:
        return READINESS_NEEDS_APPROVAL
    return READINESS_READY


def _alerts(
    *,
    preflight_status: str,
    metadata_error: str | None,
    metadata_safety: dict[str, Any],
    draft_validation: dict[str, Any],
    approval_validation: dict[str, Any],
    channel_readiness: dict[str, Any],
    config_safety: dict[str, list[str]],
    allow_missing_recipient_config: bool,
    allow_missing_channel_config: bool,
) -> dict[str, list[str]]:
    critical: list[str] = []
    warnings: list[str] = []
    notes = ["Delivery preflight is read-only and did not send any notification."]
    if metadata_error and preflight_status in {PREFLIGHT_INPUT_MISSING, PREFLIGHT_INPUT_INVALID}:
        critical.append(metadata_error)
    critical.extend(_strings(metadata_safety.get("blocking_reasons")))
    critical.extend(_strings(draft_validation.get("sensitive_findings")))
    critical.extend(_strings(config_safety.get("blocking_reasons")))
    if preflight_status == PREFLIGHT_BLOCKED:
        critical.extend(_strings(draft_validation.get("blocking_reasons")))
        critical.extend(_strings(approval_validation.get("blocking_reasons")))
    warnings.extend(_strings(approval_validation.get("warnings")))
    warnings.extend(_strings(config_safety.get("warnings")))
    for channel, record in channel_readiness.items():
        channel_warnings = _strings(_mapping(record).get("warnings"))
        for warning in channel_warnings:
            if "Recipient config" in warning and allow_missing_recipient_config:
                continue
            if "Channel config" in warning and allow_missing_channel_config:
                continue
            if "Mobile push config" in warning and allow_missing_channel_config:
                continue
            warnings.append(warning)
        for reason in _strings(_mapping(record).get("blocking_reasons")):
            if reason not in critical:
                critical.append(f"{channel}: {reason}")
    return {
        "critical": _unique_strings(critical),
        "warnings": _unique_strings(warnings),
        "notes": notes,
    }


def _headline(preflight_status: str, delivery_readiness: str) -> str:
    if preflight_status == PREFLIGHT_SAFETY_BLOCKED:
        return "Notification delivery preflight is safety blocked. No notification was sent."
    if preflight_status == PREFLIGHT_BLOCKED:
        return "Notification delivery preflight is blocked. No notification was sent."
    if delivery_readiness == READINESS_NEEDS_APPROVAL:
        return (
            "Notification drafts need approval before any delivery review. "
            "No notification was sent."
        )
    if preflight_status in {PREFLIGHT_INPUT_MISSING, PREFLIGHT_INPUT_INVALID}:
        return "Notification delivery preflight could not validate required input."
    return (
        "Notification drafts are available and ready for manual review. "
        "No notification was sent."
    )


def _recommended_next_steps(*, preflight_status: str, delivery_readiness: str) -> list[str]:
    if preflight_status == PREFLIGHT_SAFETY_BLOCKED:
        return [
            "Stop delivery review until the safety finding is resolved.",
            "Inspect the TRADING-030 metadata and draft content manually.",
            "Do not send any notification from these drafts.",
        ]
    if preflight_status == PREFLIGHT_BLOCKED:
        return [
            (
                "Restore or regenerate the missing/invalid TRADING-030 draft artifact "
                "in a separate task."
            ),
            "Review source operator brief before any manual notification.",
            "Do not use TRADING-031 to send notifications.",
        ]
    if delivery_readiness == READINESS_NEEDS_APPROVAL:
        return [
            "Review notification drafts manually.",
            "Obtain approval before using any urgent or action-level draft.",
            "Do not send urgent notifications without checking the source operator brief.",
        ]
    return [
        "Review notification drafts manually.",
        "Do not send urgent notifications without checking the source operator brief.",
        "Add recipient/channel config only in a future reviewed task.",
    ]


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("output_artifacts"))
    preflight_json = _mapping(outputs.get("preflight_json"))
    preflight_markdown = _mapping(outputs.get("preflight_markdown"))
    run_log_json = _mapping(outputs.get("run_log_json"))
    run_log_markdown = _mapping(outputs.get("run_log_markdown"))
    run_status = "ERROR" if payload.get("preflight_status") == PREFLIGHT_ERROR else "COMPLETED"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "run_status": run_status,
        "created_at": _isoformat_z(generated_at),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_delivery_preflight_only": True,
        "read_only": True,
        "preflight_status": payload.get("preflight_status"),
        "delivery_readiness": payload.get("delivery_readiness"),
        "notification_severity": payload.get("notification_severity"),
        "preflight_json": preflight_json.get("path"),
        "preflight_markdown": preflight_markdown.get("path"),
        "run_log_json": run_log_json.get("path"),
        "run_log_markdown": run_log_markdown.get("path"),
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_delivery_preflight": False,
        "notification_draft_executed_by_delivery_preflight": False,
        "pipelines_executed_by_delivery_preflight": False,
        "data_downloaded_by_delivery_preflight": False,
        "apply_executed_by_delivery_preflight": False,
        "rollback_executed_by_delivery_preflight": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
    }


def _error_payload(
    *,
    as_of: date,
    data_root: Path,
    output_json_path: Path,
    output_markdown_path: Path,
    run_log_json_path: Path,
    run_log_markdown_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    payload = build_operator_brief_notification_delivery_preflight(
        as_of=as_of,
        data_root=data_root,
        output_json_path=output_json_path,
        output_markdown_path=output_markdown_path,
        run_log_json_path=run_log_json_path,
        run_log_markdown_path=run_log_markdown_path,
        generated_at=generated_at,
    )
    payload["preflight_status"] = PREFLIGHT_ERROR
    payload["delivery_readiness"] = READINESS_UNKNOWN
    payload["headline"] = "Notification delivery preflight failed at runtime."
    payload["alerts"]["critical"] = _unique_strings([*payload["alerts"]["critical"], error])
    return payload


def _read_json_object_with_status(path: Path) -> tuple[dict[str, Any], str, str | None]:
    if not path.exists():
        return {}, STATUS_MISSING, f"JSON input not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, STATUS_INVALID, f"JSON input invalid: {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, STATUS_INVALID, f"JSON input must be an object: {path}"
    return payload, STATUS_FOUND, None


def _artifact_record(path: Path, status: str) -> dict[str, Any]:
    sha256 = _sha256_path(path) if path.exists() and path.is_file() else None
    return {
        "status": status,
        "path": str(path) if path else None,
        "sha256": sha256,
    }


def _draft_artifact_record(draft: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": draft.get("status"),
        "path": draft.get("path"),
        "sha256": draft.get("sha256"),
    }


def _sensitive_findings(content: str, *, label: str) -> list[str]:
    findings: list[str] = []
    for match in SENSITIVE_ASSIGNMENT_RE.finditer(content):
        field = match.group(1)
        raw_value = match.group(3).strip().strip("\"'")
        if _is_redacted_value(raw_value):
            continue
        findings.append(f"{label} contains unredacted sensitive field `{field}`.")
    return _unique_strings(findings)


def _is_redacted_value(value: str) -> bool:
    normalized = value.strip().strip("\"'").upper()
    return normalized in {"[REDACTED]", "REDACTED"} or normalized.startswith("[REDACTED]")


def _walk_config(value: object, prefix: str = "") -> list[tuple[str, str, object]]:
    records: list[tuple[str, str, object]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_prefix = f"{prefix}.{key_text}" if prefix else key_text
            records.append((child_prefix, key_text, child))
            records.extend(_walk_config(child, child_prefix))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            records.extend(_walk_config(child, f"{prefix}[{index}]"))
    return records


def _resolve_input_path(data_root: Path, value: Path) -> Path:
    if value.is_absolute():
        return value
    candidates = [data_root.parent / value, data_root / value, Path.cwd() / value]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    if value.parts and value.parts[0] == data_root.name:
        return data_root.parent / value
    return data_root / value


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


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _pass_fail(value: object) -> str:
    return "PASS" if value is True else "FAIL"


def _draft_row(label: str, artifact: object, note: str) -> str:
    record = _mapping(artifact)
    status = record.get("status") or STATUS_MISSING
    path = record.get("path") or ""
    display_note = note if status == STATUS_FOUND else "not ready"
    return f"| {label} | `{status}` | `{path}` | {display_note} |"


def _channel_row(label: str, record: dict[str, Any]) -> str:
    return (
        f"| {label} | `{record.get('status', PREFLIGHT_BLOCKED)}` | "
        f"`{_bool_text(record.get('manual_send_only'))}` | "
        f"{_join_or_dash(_strings(record.get('blocking_reasons')))} | "
        f"{_join_or_dash(_strings(record.get('warnings')))} |"
    )


def _join_or_dash(values: list[str]) -> str:
    return "；".join(values) if values else "-"


def _markdown_list(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _assert_delivery_preflight_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-031 production_effect must be none")
    for field in OUTPUT_SAFETY_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-031 {field} must be true")
    for field in OUTPUT_SAFETY_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-031 {field} must be false")
