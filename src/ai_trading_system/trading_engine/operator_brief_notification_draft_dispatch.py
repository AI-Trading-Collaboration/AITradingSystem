from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "operator_brief_notification_draft_dispatch"
TASK_ID = "TRADING-034"
TASK_NAME = "Operator Brief Notification Draft Dispatch"
INPUT_APPROVAL_GATE_TASK_ID = "TRADING-033"
INPUT_APPROVAL_GATE_REPORT_TYPE = "operator_brief_notification_approval_gate"
INPUT_PREVIEW_TASK_ID = "TRADING-032"
INPUT_PREVIEW_REPORT_TYPE = "operator_brief_notification_dispatch_preview"
MODE = "draft_dispatch"
PRODUCTION_EFFECT_NONE = "none"

DRAFT_READY = "DRAFT_READY"
APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
APPROVAL_EXPIRED = "APPROVAL_EXPIRED"
APPROVAL_MISMATCH = "APPROVAL_MISMATCH"
SAFETY_BLOCKED = "SAFETY_BLOCKED"
BLOCKED = "BLOCKED"
NOOP = "NOOP"

APPROVED = "APPROVED"

DISPATCH_WOULD_SEND = "WOULD_SEND"
DISPATCH_NEEDS_APPROVAL = "NEEDS_APPROVAL"
DISPATCH_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DISPATCH_BLOCKED = "BLOCKED"
DISPATCH_NOOP = "NOOP"

STATUS_FOUND = "FOUND"
STATUS_MISSING = "MISSING"
STATUS_INVALID = "INVALID"
STATUS_UNSAFE = "UNSAFE"

HASH_ALGORITHM = "sha256"
DISPATCH_PREVIEW_HASH_SCOPE = "canonical_dispatch_preview_json"
DRAFT_HASH_SCOPE = "canonical_draft_dispatch_json"
HASH_VOLATILE_KEYS = {
    "generated_at",
    "preview_generated_at",
    "created_at",
    "updated_at",
    "output_artifacts",
    "run_log",
    "path",
    "href",
    "sha256",
    "draft_hash",
    "draft_id",
}
PREVIEW_HASH_VOLATILE_KEYS = {
    "generated_at",
    "preview_generated_at",
    "created_at",
    "updated_at",
    "output_artifacts",
    "run_log",
}

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

SAFETY_TRUE_FIELDS = (
    "manual_review_only",
    "draft_dispatch_only",
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
    "smtp_called",
    "slack_sent",
    "telegram_sent",
    "discord_sent",
    "webhook_called",
    "mobile_push_sent",
    "operator_brief_executed_by_draft_dispatch",
    "notification_draft_executed_by_draft_dispatch",
    "delivery_preflight_executed_by_draft_dispatch",
    "dispatch_preview_executed_by_draft_dispatch",
    "approval_gate_executed_by_draft_dispatch",
    "pipelines_executed_by_draft_dispatch",
    "data_downloaded_by_draft_dispatch",
    "apply_executed_by_draft_dispatch",
    "rollback_executed_by_draft_dispatch",
    "operator_brief_executed_by_dispatch",
    "pipelines_executed_by_dispatch",
    "data_downloaded_by_dispatch",
    "apply_executed_by_dispatch",
    "rollback_executed_by_dispatch",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"\b(api_key|apiKey|secret|token|password|credential|broker_token|account_id|"
    r"private_key)\b\s*([:=])\s*(\"[^\"]*\"|'[^']*'|\[[^\]]+\]|[^\s,;`]+)",
    re.IGNORECASE,
)
PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----", re.IGNORECASE)


def default_notification_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs" / "notifications"


def default_dispatch_preview_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "dispatch_preview"


def default_approval_gate_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "approval_gate"


def default_draft_dispatch_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "draft_dispatch"


def default_draft_dispatch_json_path(data_root: Path, as_of: date) -> Path:
    return default_draft_dispatch_root(data_root) / (
        f"operator_brief_notification_draft_dispatch_{as_of.isoformat()}.json"
    )


def default_draft_dispatch_latest_json_path(data_root: Path) -> Path:
    return default_draft_dispatch_root(data_root) / "latest.json"


def default_draft_dispatch_latest_markdown_path(data_root: Path) -> Path:
    return default_draft_dispatch_root(data_root) / "latest.md"


def default_draft_dispatch_run_log_path(data_root: Path) -> Path:
    return default_draft_dispatch_root(data_root) / "run.log"


def write_operator_brief_notification_draft_dispatch(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    input_approval_gate_file: Path | None = None,
    input_dispatch_preview_file: Path | None = None,
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
    json_path = output_root / f"operator_brief_notification_draft_dispatch_{as_of.isoformat()}.json"
    markdown_path = json_path.with_suffix(".md")
    latest_json_path = output_root / "latest.json"
    latest_markdown_path = output_root / "latest.md"
    run_log_path = output_root / "run.log"

    payload = build_operator_brief_notification_draft_dispatch(
        as_of=as_of,
        data_root=data_root,
        input_approval_gate_file=input_approval_gate_file,
        input_dispatch_preview_file=input_dispatch_preview_file,
        output_json_path=json_path,
        output_markdown_path=markdown_path,
        latest_json_path=latest_json_path,
        latest_markdown_path=latest_markdown_path,
        run_log_path=run_log_path,
        generated_at=generated,
        output_safety_finding=output_safety_finding,
    )
    markdown = render_operator_brief_notification_draft_dispatch_markdown(payload)
    run_log = render_operator_brief_notification_draft_dispatch_run_log(payload)
    _write_json(json_path, payload)
    _write_text(markdown_path, markdown)
    _write_json(latest_json_path, payload)
    _write_text(latest_markdown_path, markdown)
    _write_text(run_log_path, run_log)
    return payload


def build_operator_brief_notification_draft_dispatch(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    input_approval_gate_file: Path | None = None,
    input_dispatch_preview_file: Path | None = None,
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
    output_json_path = output_json_path or default_draft_dispatch_json_path(data_root, as_of)
    output_markdown_path = output_markdown_path or output_json_path.with_suffix(".md")
    latest_json_path = latest_json_path or default_draft_dispatch_latest_json_path(data_root)
    latest_markdown_path = latest_markdown_path or default_draft_dispatch_latest_markdown_path(
        data_root
    )
    run_log_path = run_log_path or default_draft_dispatch_run_log_path(data_root)

    path_safety_findings: list[str] = []
    if output_safety_finding:
        path_safety_findings.append(output_safety_finding)

    gate_path, gate_path_finding = _resolve_approval_gate_path(
        as_of=as_of,
        data_root=data_root,
        project_root=project_root,
        explicit_path=input_approval_gate_file,
    )
    if gate_path_finding:
        path_safety_findings.append(gate_path_finding)
    gate_payload, gate_artifact_status, gate_error = _read_json_object_with_status(
        gate_path,
        unsafe=bool(gate_path_finding),
    )

    preview_path, preview_path_finding = _resolve_dispatch_preview_path(
        as_of=as_of,
        data_root=data_root,
        project_root=project_root,
        explicit_path=input_dispatch_preview_file,
    )
    if preview_path_finding:
        path_safety_findings.append(preview_path_finding)
    preview_payload, preview_artifact_status, preview_error = _read_json_object_with_status(
        preview_path,
        unsafe=bool(preview_path_finding),
    )

    reasons: list[str] = []
    warnings: list[str] = []
    if gate_error:
        reasons.append(gate_error)
    if preview_error:
        reasons.append(preview_error)
    warnings.extend(path_safety_findings)

    gate_schema_valid = _approval_gate_schema_valid(
        gate_payload=gate_payload,
        artifact_status=gate_artifact_status,
    )
    preview_schema_valid = _dispatch_preview_schema_valid(
        preview_payload=preview_payload,
        artifact_status=preview_artifact_status,
    )
    if gate_artifact_status == STATUS_FOUND and not gate_schema_valid:
        reasons.append("TRADING-033 approval gate artifact has an unexpected schema.")
    if preview_artifact_status == STATUS_FOUND and not preview_schema_valid:
        reasons.append("TRADING-032 dispatch preview artifact has an unexpected schema.")

    current_preview_hash = ""
    hash_error = ""
    if preview_schema_valid:
        try:
            current_preview_hash = compute_dispatch_preview_hash(preview_payload)
        except (TypeError, ValueError, OSError) as exc:
            hash_error = f"Dispatch preview hash calculation failed: {exc}"
            reasons.append(hash_error)

    gate_summary = _approval_gate_summary(gate_payload)
    preview_summary = _dispatch_preview_summary(preview_payload)
    gate_status = _string_value(gate_summary.get("approval_gate_status")) or BLOCKED
    preview_status = _string_value(preview_summary.get("final_status")) or BLOCKED
    gate_hash = _string_value(gate_summary.get("dispatch_preview_hash"))

    draft_message, message_findings = _draft_message_from_preview(preview_payload)
    reasons.extend(message_findings["reasons"])
    warnings.extend(message_findings["warnings"])
    draft_channels, channel_findings = _draft_channels_from_preview(preview_payload)
    reasons.extend(channel_findings["reasons"])
    warnings.extend(channel_findings["warnings"])

    safety_findings = list(path_safety_findings)
    safety_findings.extend(_preview_safety_findings(preview_payload, preview_status))
    safety_findings.extend(message_findings["safety_findings"])
    safety_findings.extend(channel_findings["safety_findings"])
    safety_findings = _unique_strings(safety_findings)

    final_status = _draft_status(
        gate_artifact_status=gate_artifact_status,
        gate_schema_valid=gate_schema_valid,
        preview_artifact_status=preview_artifact_status,
        preview_schema_valid=preview_schema_valid,
        gate_status=gate_status,
        allowed_to_enter_dispatch=gate_summary.get("allowed_to_enter_dispatch") is True,
        gate_hash=gate_hash,
        current_preview_hash=current_preview_hash,
        preview_status=preview_status,
        hash_error=hash_error,
        path_safety_findings=path_safety_findings,
        safety_findings=safety_findings,
        subject=draft_message["subject"],
        body=draft_message["body_markdown"],
        channels=draft_channels,
    )
    reasons.extend(
        _decision_reasons(
            final_status=final_status,
            gate_status=gate_status,
            preview_status=preview_status,
            gate_artifact_status=gate_artifact_status,
            gate_schema_valid=gate_schema_valid,
            preview_artifact_status=preview_artifact_status,
            preview_schema_valid=preview_schema_valid,
            gate_hash=gate_hash,
            current_preview_hash=current_preview_hash,
        )
    )
    reasons = _unique_strings(reasons)
    warnings = _unique_strings(warnings)

    ready_for_actual_dispatch = final_status == DRAFT_READY
    draft_ready_channel_count = sum(
        1 for channel in draft_channels if channel.get("draft_ready") is True
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "draft_dispatch_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "external_side_effects": False,
        "network_access_required": False,
        "secrets_required": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "smtp_called": False,
        "slack_sent": False,
        "telegram_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_draft_dispatch": False,
        "notification_draft_executed_by_draft_dispatch": False,
        "delivery_preflight_executed_by_draft_dispatch": False,
        "dispatch_preview_executed_by_draft_dispatch": False,
        "approval_gate_executed_by_draft_dispatch": False,
        "pipelines_executed_by_draft_dispatch": False,
        "data_downloaded_by_draft_dispatch": False,
        "apply_executed_by_draft_dispatch": False,
        "rollback_executed_by_draft_dispatch": False,
        "operator_brief_executed_by_dispatch": False,
        "pipelines_executed_by_dispatch": False,
        "data_downloaded_by_dispatch": False,
        "apply_executed_by_dispatch": False,
        "rollback_executed_by_dispatch": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": TASK_ID,
            "task_name": TASK_NAME,
            "run_date": as_of.isoformat(),
            "generated_at": _isoformat_z(generated),
            "mode": MODE,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
        },
        "input_refs": {
            "approval_gate_artifact": _input_ref(gate_path, gate_artifact_status),
            "dispatch_preview_artifact": _input_ref(preview_path, preview_artifact_status),
        },
        "approval_gate_summary": {
            "approval_gate_status": gate_status,
            "allowed_to_enter_dispatch": gate_summary.get("allowed_to_enter_dispatch") is True,
            "dispatch_preview_hash": gate_hash,
            "current_dispatch_preview_hash": current_preview_hash,
        },
        "dispatch_preview_summary": preview_summary,
        "draft": {
            "draft_status": final_status,
            "draft_id": "",
            "channel_count": len(draft_channels),
            "draft_ready_channel_count": draft_ready_channel_count,
            "channels": draft_channels,
            "message": draft_message,
        },
        "hashes": {
            "dispatch_preview_hash": current_preview_hash,
            "approval_gate_dispatch_preview_hash": gate_hash,
            "draft_hash": "",
            "hash_algorithm": HASH_ALGORITHM,
            "hash_scope": DRAFT_HASH_SCOPE,
            "dispatch_preview_hash_scope": DISPATCH_PREVIEW_HASH_SCOPE,
        },
        "decision": {
            "final_status": final_status,
            "ready_for_actual_dispatch": ready_for_actual_dispatch,
            "human_action_required": final_status != NOOP,
            "next_recommended_action": _next_recommended_action(final_status=final_status),
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "recipient_masking_applied": True,
            "approval_gate_required": True,
            "approval_gate_passed": (
                gate_status == APPROVED
                and gate_summary.get("allowed_to_enter_dispatch") is True
                and final_status == DRAFT_READY
            ),
            "path_safety_findings": path_safety_findings,
            "sensitive_content_flags": safety_findings,
        },
        "reasons": reasons,
        "warnings": warnings,
        "audit": {
            "created_by": "scripts/run_operator_brief_notification_draft_dispatch.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_draft_dispatch_artifacts": True,
        },
        "output_artifacts": {
            "draft_dispatch_json": {"path": str(output_json_path)},
            "draft_dispatch_markdown": {"path": str(output_markdown_path)},
            "latest_json": {"path": str(latest_json_path)},
            "latest_markdown": {"path": str(latest_markdown_path)},
            "run_log": {"path": str(run_log_path)},
        },
    }
    draft_hash = compute_draft_dispatch_hash(payload)
    payload["hashes"]["draft_hash"] = draft_hash
    payload["draft"]["draft_id"] = f"local-draft-{draft_hash.removeprefix('sha256:')[:16]}"
    _assert_draft_dispatch_safety_invariants(payload)
    return payload


def compute_dispatch_preview_hash(payload: dict[str, Any]) -> str:
    canonical_payload = _canonical_payload(payload, volatile_keys=PREVIEW_HASH_VOLATILE_KEYS)
    canonical_json = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return f"{HASH_ALGORITHM}:{digest}"


def compute_draft_dispatch_hash(payload: dict[str, Any]) -> str:
    canonical_payload = _canonical_payload(payload, volatile_keys=HASH_VOLATILE_KEYS)
    canonical_json = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return f"{HASH_ALGORITHM}:{digest}"


def render_operator_brief_notification_draft_dispatch_markdown(
    payload: dict[str, Any],
) -> str:
    metadata = _mapping(payload.get("metadata"))
    gate = _mapping(payload.get("approval_gate_summary"))
    draft = _mapping(payload.get("draft"))
    message = _mapping(draft.get("message"))
    hashes = _mapping(payload.get("hashes"))
    decision = _mapping(payload.get("decision"))
    safety = _mapping(payload.get("safety"))
    channels = _records(draft.get("channels"))
    lines = [
        "# Operator Brief Notification Draft Dispatch",
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
        f"- Final status: `{decision.get('final_status', BLOCKED)}`",
        "- Ready for actual dispatch: "
        f"`{_bool_text(decision.get('ready_for_actual_dispatch'))}`",
        f"- Human action required: `{_bool_text(decision.get('human_action_required'))}`",
        f"- Next recommended action: {decision.get('next_recommended_action') or ''}",
        "",
        "## Approval Gate Summary",
        "",
        f"- Approval gate status: `{gate.get('approval_gate_status', BLOCKED)}`",
        "- Allowed to enter dispatch: " f"`{_bool_text(gate.get('allowed_to_enter_dispatch'))}`",
        f"- Dispatch preview hash: `{gate.get('dispatch_preview_hash') or ''}`",
        "",
        "## Draft Summary",
        "",
        f"- Draft ID: `{draft.get('draft_id') or ''}`",
        f"- Channel count: `{draft.get('channel_count', 0)}`",
        f"- Draft-ready channel count: `{draft.get('draft_ready_channel_count', 0)}`",
        f"- Subject: {message.get('subject') or ''}",
        f"- Body length: `{message.get('body_length', 0)}`",
        "",
        "## Channels",
        "",
        *_render_channel_sections(channels),
        "",
        "## Message Draft",
        "",
        "### Subject",
        "",
        message.get("subject") or "",
        "",
        "### Body",
        "",
        message.get("body_markdown") or "",
        "",
        "## Hashes",
        "",
        f"- Dispatch preview hash: `{hashes.get('dispatch_preview_hash') or ''}`",
        f"- Draft hash: `{hashes.get('draft_hash') or ''}`",
        f"- Scope: `{hashes.get('hash_scope', DRAFT_HASH_SCOPE)}`",
        "",
        "## Safety",
        "",
        f"- External side effects: `{_bool_text(safety.get('external_side_effects'))}`",
        f"- Network access required: `{_bool_text(safety.get('network_access_required'))}`",
        f"- Secrets required: `{_bool_text(safety.get('secrets_required'))}`",
        "- Recipient masking applied: " f"`{_bool_text(safety.get('recipient_masking_applied'))}`",
        f"- Approval gate required: `{_bool_text(safety.get('approval_gate_required'))}`",
        f"- Approval gate passed: `{_bool_text(safety.get('approval_gate_passed'))}`",
        "",
        "## Reasons",
        "",
        *_markdown_list(_strings(payload.get("reasons"))),
        "",
        "## Warnings",
        "",
        *_markdown_list(_strings(payload.get("warnings"))),
        "",
    ]
    return "\n".join(lines)


def render_operator_brief_notification_draft_dispatch_run_log(
    payload: dict[str, Any],
) -> str:
    decision = _mapping(payload.get("decision"))
    gate = _mapping(payload.get("approval_gate_summary"))
    draft = _mapping(payload.get("draft"))
    hashes = _mapping(payload.get("hashes"))
    outputs = _mapping(payload.get("output_artifacts"))
    return "\n".join(
        [
            f"Operator Brief Notification Draft Dispatch Run - {payload.get('date')}",
            "run_status=COMPLETED",
            f"final_status={decision.get('final_status')}",
            f"ready_for_actual_dispatch={_bool_text(decision.get('ready_for_actual_dispatch'))}",
            f"approval_gate_status={gate.get('approval_gate_status')}",
            f"allowed_to_enter_dispatch={_bool_text(gate.get('allowed_to_enter_dispatch'))}",
            f"channel_count={draft.get('channel_count')}",
            f"draft_ready_channel_count={draft.get('draft_ready_channel_count')}",
            f"dispatch_preview_hash={hashes.get('dispatch_preview_hash')}",
            f"draft_hash={hashes.get('draft_hash')}",
            f"production_effect={payload.get('production_effect')}",
            f"manual_review_only={_bool_text(payload.get('manual_review_only'))}",
            f"draft_dispatch_only={_bool_text(payload.get('draft_dispatch_only'))}",
            f"read_only={_bool_text(payload.get('read_only'))}",
            f"external_side_effects={_bool_text(payload.get('external_side_effects'))}",
            f"network_access_required={_bool_text(payload.get('network_access_required'))}",
            f"secrets_required={_bool_text(payload.get('secrets_required'))}",
            f"email_sent={_bool_text(payload.get('email_sent'))}",
            f"gmail_draft_created={_bool_text(payload.get('gmail_draft_created'))}",
            f"gmail_draft_modified={_bool_text(payload.get('gmail_draft_modified'))}",
            f"smtp_called={_bool_text(payload.get('smtp_called'))}",
            f"slack_sent={_bool_text(payload.get('slack_sent'))}",
            f"telegram_sent={_bool_text(payload.get('telegram_sent'))}",
            f"discord_sent={_bool_text(payload.get('discord_sent'))}",
            f"webhook_called={_bool_text(payload.get('webhook_called'))}",
            f"mobile_push_sent={_bool_text(payload.get('mobile_push_sent'))}",
            f"broker_execution={_bool_text(payload.get('broker_execution'))}",
            f"replay_execution={_bool_text(payload.get('replay_execution'))}",
            f"trading_execution={_bool_text(payload.get('trading_execution'))}",
            f"draft_dispatch_json={_path_from_ref(outputs.get('draft_dispatch_json'))}",
            f"draft_dispatch_markdown={_path_from_ref(outputs.get('draft_dispatch_markdown'))}",
            f"latest_json={_path_from_ref(outputs.get('latest_json'))}",
            f"latest_markdown={_path_from_ref(outputs.get('latest_markdown'))}",
            f"run_log={_path_from_ref(outputs.get('run_log'))}",
            "",
        ]
    )


def should_fail_cli(payload: dict[str, Any]) -> bool:
    return payload.get("runtime_error") is True


def _resolve_approval_gate_path(
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
            purpose="approval gate artifact",
        )
    gate_root = default_approval_gate_root(data_root)
    latest_path = gate_root / "latest.json"
    if latest_path.exists():
        return latest_path, None
    default_path = gate_root / f"operator_brief_notification_approval_gate_{as_of}.json"
    return (
        _latest_dated_or_latest(
            root=gate_root,
            prefix="operator_brief_notification_approval_gate_",
            as_of=as_of,
            default_path=default_path,
            latest_name="latest.json",
        ),
        None,
    )


def _resolve_dispatch_preview_path(
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
            purpose="dispatch preview artifact",
        )
    preview_root = default_dispatch_preview_root(data_root)
    latest_path = preview_root / "latest.json"
    if latest_path.exists():
        return latest_path, None
    default_path = preview_root / f"operator_brief_notification_dispatch_preview_{as_of}.json"
    return (
        _latest_dated_or_latest(
            root=preview_root,
            prefix="operator_brief_notification_dispatch_preview_",
            as_of=as_of,
            default_path=default_path,
            latest_name="latest.json",
        ),
        None,
    )


def _resolve_output_root(
    *,
    data_root: Path,
    project_root: Path,
    output_dir: Path | None,
) -> tuple[Path, str | None]:
    if output_dir is None:
        return default_draft_dispatch_root(data_root), None
    path, finding = _resolve_repo_path(
        value=output_dir,
        data_root=data_root,
        project_root=project_root,
        purpose="draft dispatch output directory",
        allow_missing=True,
    )
    if finding:
        return default_draft_dispatch_root(data_root), finding
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
        return resolved, f"{purpose} path escapes repo root and was not read: {resolved}"
    if any(part.lower().startswith(".env") for part in resolved.parts):
        return resolved, f"{purpose} path points to .env secrets and was not read: {resolved}"
    return resolved, None


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


def _approval_gate_schema_valid(
    *,
    gate_payload: dict[str, Any],
    artifact_status: str,
) -> bool:
    return (
        artifact_status == STATUS_FOUND
        and gate_payload.get("report_type") == INPUT_APPROVAL_GATE_REPORT_TYPE
        and gate_payload.get("task_id") == INPUT_APPROVAL_GATE_TASK_ID
        and isinstance(gate_payload.get("decision"), dict)
        and isinstance(gate_payload.get("hashes"), dict)
    )


def _dispatch_preview_schema_valid(
    *,
    preview_payload: dict[str, Any],
    artifact_status: str,
) -> bool:
    return (
        artifact_status == STATUS_FOUND
        and preview_payload.get("report_type") == INPUT_PREVIEW_REPORT_TYPE
        and preview_payload.get("task_id") == INPUT_PREVIEW_TASK_ID
        and isinstance(preview_payload.get("dispatch_preview"), dict)
        and isinstance(preview_payload.get("decision"), dict)
    )


def _approval_gate_summary(gate_payload: dict[str, Any]) -> dict[str, Any]:
    decision = _mapping(gate_payload.get("decision"))
    hashes = _mapping(gate_payload.get("hashes"))
    return {
        "approval_gate_status": _string_value(decision.get("approval_gate_status")) or BLOCKED,
        "allowed_to_enter_dispatch": decision.get("allowed_to_enter_dispatch") is True,
        "dispatch_preview_hash": _string_value(hashes.get("dispatch_preview_hash")),
    }


def _dispatch_preview_summary(preview_payload: dict[str, Any]) -> dict[str, Any]:
    preview = _mapping(preview_payload.get("dispatch_preview"))
    decision = _mapping(preview_payload.get("decision"))
    channels = _records(preview.get("channels"))
    final_status = _string_value(decision.get("final_status")) or _string_value(
        preview.get("dispatch_status")
    )
    return {
        "final_status": final_status or BLOCKED,
        "dispatch_status": _string_value(preview.get("dispatch_status")) or final_status,
        "human_action_required": decision.get("human_action_required") is True,
        "channel_count": len(channels),
        "would_send_channel_count": sum(
            1 for channel in channels if channel.get("would_send") is True
        ),
    }


def _draft_message_from_preview(
    preview_payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    findings: dict[str, list[str]] = {"reasons": [], "warnings": [], "safety_findings": []}
    preview = _mapping(preview_payload.get("dispatch_preview"))
    source_message = _mapping(preview.get("message"))
    subject = _redact_sensitive_text(
        _string_value(source_message.get("subject"))
        or _string_value(source_message.get("subject_preview"))
    )
    title = _redact_sensitive_text(
        _string_value(source_message.get("title"))
        or _string_value(source_message.get("title_preview"))
    )
    body_source = "body_markdown"
    body = _string_value(source_message.get("body_markdown")) or _string_value(
        source_message.get("body")
    )
    if not body:
        body_source = "body_excerpt"
        body = _string_value(source_message.get("body_excerpt"))
        if body:
            findings["warnings"].append(
                "TRADING-032 did not provide full body_markdown; draft uses body_excerpt."
            )
    if not subject:
        findings["reasons"].append("Message subject is missing from TRADING-032 preview.")
    if not body:
        findings["reasons"].append("Message body is missing from TRADING-032 preview.")
    raw_text = "\n".join(
        [
            _string_value(source_message.get("subject")),
            _string_value(source_message.get("subject_preview")),
            _string_value(source_message.get("body_markdown")),
            _string_value(source_message.get("body")),
            _string_value(source_message.get("body_excerpt")),
        ]
    )
    if _contains_sensitive_text(raw_text):
        findings["safety_findings"].append("Message content contains sensitive token material.")
    body = _redact_sensitive_text(body)
    return (
        {
            "subject": subject,
            "title": title,
            "body_markdown": body,
            "body_length": len(body),
            "contains_markdown": source_message.get("contains_markdown") is True
            or _looks_like_markdown(body),
            "body_source": body_source,
        },
        findings,
    )


def _draft_channels_from_preview(
    preview_payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    findings: dict[str, list[str]] = {"reasons": [], "warnings": [], "safety_findings": []}
    preview = _mapping(preview_payload.get("dispatch_preview"))
    source_channels = _records(preview.get("channels"))
    if not source_channels:
        findings["reasons"].append("Channel configuration is missing from TRADING-032 preview.")
    channels: list[dict[str, Any]] = []
    for index, channel in enumerate(source_channels, start=1):
        raw_target = _string_value(channel.get("target_ref"))
        masked_target = _mask_target(raw_target)
        unmasked_target = _has_unmasked_email(raw_target)
        enabled = channel.get("enabled") is True
        would_send = channel.get("would_send") is True
        draft_ready = enabled and would_send and not unmasked_target
        reason = _string_value(channel.get("reason"))
        if unmasked_target:
            findings["safety_findings"].append(
                f"Channel {index} target_ref was not masked in TRADING-032 preview."
            )
            reason = "Target reference was not masked; draft dispatch is safety blocked."
        channels.append(
            {
                "channel_type": _string_value(channel.get("channel_type")) or "unknown",
                "target_ref": masked_target,
                "enabled": enabled,
                "draft_ready": draft_ready,
                "reason": reason,
            }
        )
    if source_channels and not any(channel.get("enabled") is True for channel in source_channels):
        findings["reasons"].append("No enabled notification channel is available.")
    if source_channels and not any(channel.get("draft_ready") is True for channel in channels):
        findings["reasons"].append("No channel is draft-ready for dispatch.")
    return channels, findings


def _preview_safety_findings(preview_payload: dict[str, Any], preview_status: str) -> list[str]:
    safety = _mapping(preview_payload.get("safety"))
    findings: list[str] = []
    if preview_status == DISPATCH_SAFETY_BLOCKED:
        findings.append("TRADING-032 dispatch preview is safety blocked.")
    if safety.get("external_side_effects") is True:
        findings.append("TRADING-032 preview requested external side effects.")
    if safety.get("network_access_required") is True:
        findings.append("TRADING-032 preview requires network access.")
    if safety.get("secrets_required") is True:
        findings.append("TRADING-032 preview requires secrets.")
    findings.extend(_strings(safety.get("sensitive_content_flags")))
    return findings


def _draft_status(
    *,
    gate_artifact_status: str,
    gate_schema_valid: bool,
    preview_artifact_status: str,
    preview_schema_valid: bool,
    gate_status: str,
    allowed_to_enter_dispatch: bool,
    gate_hash: str,
    current_preview_hash: str,
    preview_status: str,
    hash_error: str,
    path_safety_findings: list[str],
    safety_findings: list[str],
    subject: str,
    body: str,
    channels: list[dict[str, Any]],
) -> str:
    if path_safety_findings and (
        gate_artifact_status == STATUS_UNSAFE
        or preview_artifact_status == STATUS_UNSAFE
        or path_safety_findings
    ):
        return SAFETY_BLOCKED
    if not gate_schema_valid or gate_artifact_status != STATUS_FOUND:
        return BLOCKED
    if not preview_schema_valid or preview_artifact_status != STATUS_FOUND:
        return BLOCKED
    if hash_error:
        return BLOCKED
    if (
        safety_findings
        or gate_status == SAFETY_BLOCKED
        or preview_status == DISPATCH_SAFETY_BLOCKED
    ):
        return SAFETY_BLOCKED
    if gate_status == NOOP and preview_status == DISPATCH_NOOP:
        return NOOP
    if gate_status == APPROVAL_EXPIRED:
        return APPROVAL_EXPIRED
    if gate_status == APPROVAL_MISMATCH:
        return APPROVAL_MISMATCH
    if gate_hash and current_preview_hash and gate_hash != current_preview_hash:
        return APPROVAL_MISMATCH
    if gate_status == APPROVAL_REQUIRED or preview_status == DISPATCH_NEEDS_APPROVAL:
        return APPROVAL_REQUIRED
    if gate_status == BLOCKED or preview_status == DISPATCH_BLOCKED:
        return BLOCKED
    if gate_status != APPROVED:
        return BLOCKED
    if allowed_to_enter_dispatch is not True:
        return APPROVAL_REQUIRED
    if not gate_hash or not current_preview_hash:
        return BLOCKED
    if preview_status != DISPATCH_WOULD_SEND:
        return BLOCKED
    if not subject or not body:
        return BLOCKED
    if not any(channel.get("draft_ready") is True for channel in channels):
        return BLOCKED
    return DRAFT_READY


def _decision_reasons(
    *,
    final_status: str,
    gate_status: str,
    preview_status: str,
    gate_artifact_status: str,
    gate_schema_valid: bool,
    preview_artifact_status: str,
    preview_schema_valid: bool,
    gate_hash: str,
    current_preview_hash: str,
) -> list[str]:
    if final_status == DRAFT_READY:
        return ["Approved TRADING-033 gate and TRADING-032 preview are aligned."]
    if final_status == APPROVAL_REQUIRED:
        return ["Approval gate has not approved the current dispatch preview."]
    if final_status == APPROVAL_EXPIRED:
        return ["TRADING-033 approval gate has expired."]
    if final_status == APPROVAL_MISMATCH:
        if gate_hash and current_preview_hash and gate_hash != current_preview_hash:
            return ["TRADING-032 current preview hash differs from TRADING-033 gate hash."]
        return ["TRADING-033 approval gate reported APPROVAL_MISMATCH."]
    if final_status == SAFETY_BLOCKED:
        return ["Safety findings prevent draft dispatch readiness."]
    if final_status == NOOP:
        return ["TRADING-033 and TRADING-032 are NOOP; no operator brief dispatch is needed."]
    if gate_artifact_status != STATUS_FOUND or not gate_schema_valid:
        return ["TRADING-033 approval gate artifact is missing, invalid, or not trusted."]
    if preview_artifact_status != STATUS_FOUND or not preview_schema_valid:
        return ["TRADING-032 dispatch preview artifact is missing, invalid, or not trusted."]
    if gate_status != APPROVED:
        return [f"TRADING-033 approval gate status is {gate_status}."]
    if preview_status != DISPATCH_WOULD_SEND:
        return [f"TRADING-032 dispatch preview status is {preview_status}."]
    return ["Draft dispatch cannot be made ready from the current inputs."]


def _next_recommended_action(*, final_status: str) -> str:
    if final_status == DRAFT_READY:
        return (
            "Review this local draft dispatch artifact before any future actual dispatch task; "
            "TRADING-034 sent nothing."
        )
    if final_status == APPROVAL_REQUIRED:
        return "Approve the current TRADING-032 dispatch preview through TRADING-033 first."
    if final_status == APPROVAL_EXPIRED:
        return "Re-review the dispatch preview and create a fresh approval gate artifact."
    if final_status == APPROVAL_MISMATCH:
        return "Regenerate or re-approve the dispatch preview so hashes align."
    if final_status == SAFETY_BLOCKED:
        return "Resolve safety findings before creating a ready draft dispatch."
    if final_status == NOOP:
        return "No operator brief notification dispatch is needed for this run."
    return "Restore valid TRADING-032 and TRADING-033 artifacts before draft dispatch."


def _canonical_payload(value: object, *, volatile_keys: set[str]) -> object:
    if isinstance(value, dict):
        return {
            key: _canonical_payload(child, volatile_keys=volatile_keys)
            for key, child in sorted(value.items())
            if key not in volatile_keys
        }
    if isinstance(value, list):
        return [_canonical_payload(child, volatile_keys=volatile_keys) for child in value]
    return value


def _input_ref(path: Path, status: str) -> dict[str, Any]:
    return {
        "path": str(path),
        "status": status,
        "sha256": _sha256_path(path) if status == STATUS_FOUND and path.is_file() else None,
    }


def _path_from_ref(value: object) -> str:
    return _string_value(_mapping(value).get("path"))


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


def _normalize_datetime(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


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
    normalized = _normalize_datetime(value)
    return normalized.isoformat().replace("+00:00", "Z")


def _bool_text(value: object) -> str:
    return "true" if value is True else "false"


def _markdown_list(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _render_channel_sections(channels: list[dict[str, Any]]) -> list[str]:
    if not channels:
        return ["- None."]
    lines: list[str] = []
    for index, channel in enumerate(channels, start=1):
        lines.extend(
            [
                f"### Channel {index}",
                "",
                f"- Type: `{channel.get('channel_type') or 'unknown'}`",
                f"- Target: `{channel.get('target_ref') or ''}`",
                f"- Enabled: `{_bool_text(channel.get('enabled'))}`",
                f"- Draft ready: `{_bool_text(channel.get('draft_ready'))}`",
                f"- Reason: {channel.get('reason') or ''}",
                "",
            ]
        )
    return lines


def _mask_target(value: str) -> str:
    return _mask_email(_redact_sensitive_text(value))


def _mask_email(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        email = match.group(0)
        local, domain = email.split("@", 1)
        prefix = local[:1] if local else "x"
        return f"{prefix}***@{domain}"

    return EMAIL_RE.sub(replace, value)


def _has_unmasked_email(value: str) -> bool:
    return bool(EMAIL_RE.search(value))


def _contains_sensitive_text(value: str) -> bool:
    return bool(SENSITIVE_ASSIGNMENT_RE.search(value) or PRIVATE_KEY_RE.search(value))


def _redact_sensitive_text(value: str) -> str:
    if not value:
        return ""
    redacted = SENSITIVE_ASSIGNMENT_RE.sub("[REDACTED_SECRET]", value)
    if PRIVATE_KEY_RE.search(redacted):
        return "[REDACTED: private key material blocked]"
    return redacted


def _looks_like_markdown(value: str) -> bool:
    return any(token in value for token in ("# ", "## ", "- ", "* ", "`", "|"))


def _assert_draft_dispatch_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-034 production_effect must be none")
    for field in SAFETY_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-034 {field} must be true")
    for field in SAFETY_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-034 {field} must be false")
    if payload.get("decision", {}).get("final_status") == DRAFT_READY:
        gate = _mapping(payload.get("approval_gate_summary"))
        if gate.get("approval_gate_status") != APPROVED:
            raise ValueError("TRADING-034 DRAFT_READY requires APPROVED gate")
        if gate.get("allowed_to_enter_dispatch") is not True:
            raise ValueError("TRADING-034 DRAFT_READY requires allowed_to_enter_dispatch")
    serialized = json.dumps(payload, ensure_ascii=False).lower()
    forbidden_tokens = ("sent_at", "gmail_token", "api_key=", "password=", "private key-----")
    if any(token in serialized for token in forbidden_tokens):
        raise ValueError("TRADING-034 must not emit dispatch side effects or secrets")
