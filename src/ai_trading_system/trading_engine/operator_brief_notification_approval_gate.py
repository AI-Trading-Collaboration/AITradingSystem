from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "operator_brief_notification_approval_gate"
TASK_ID = "TRADING-033"
TASK_NAME = "Operator Brief Notification Approval Gate"
INPUT_PREVIEW_TASK_ID = "TRADING-032"
INPUT_PREVIEW_REPORT_TYPE = "operator_brief_notification_dispatch_preview"
MODE = "approval_gate"
PRODUCTION_EFFECT_NONE = "none"

APPROVED = "APPROVED"
APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
APPROVAL_EXPIRED = "APPROVAL_EXPIRED"
APPROVAL_MISMATCH = "APPROVAL_MISMATCH"
SAFETY_BLOCKED = "SAFETY_BLOCKED"
BLOCKED = "BLOCKED"
NOOP = "NOOP"

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
HASH_SCOPE = "canonical_dispatch_preview_json"
HASH_VOLATILE_KEYS = {
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
    "approval_gate_only",
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
    "operator_brief_executed_by_approval_gate",
    "notification_draft_executed_by_approval_gate",
    "delivery_preflight_executed_by_approval_gate",
    "dispatch_preview_executed_by_approval_gate",
    "pipelines_executed_by_approval_gate",
    "data_downloaded_by_approval_gate",
    "apply_executed_by_approval_gate",
    "rollback_executed_by_approval_gate",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)


def default_notification_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs" / "notifications"


def default_dispatch_preview_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "dispatch_preview"


def default_approval_gate_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "approval_gate"


def default_approval_gate_json_path(data_root: Path, as_of: date) -> Path:
    return default_approval_gate_root(data_root) / (
        f"operator_brief_notification_approval_gate_{as_of.isoformat()}.json"
    )


def default_approval_gate_latest_json_path(data_root: Path) -> Path:
    return default_approval_gate_root(data_root) / "latest.json"


def default_approval_gate_latest_markdown_path(data_root: Path) -> Path:
    return default_approval_gate_root(data_root) / "latest.md"


def default_approval_gate_run_log_path(data_root: Path) -> Path:
    return default_approval_gate_root(data_root) / "run.log"


def default_approval_marker_path(data_root: Path) -> Path:
    return default_approval_gate_root(data_root) / "approval_marker.json"


def write_operator_brief_notification_approval_gate(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    input_preview_file: Path | None = None,
    approval_marker_file: Path | None = None,
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
    json_path = output_root / f"operator_brief_notification_approval_gate_{as_of.isoformat()}.json"
    markdown_path = json_path.with_suffix(".md")
    latest_json_path = output_root / "latest.json"
    latest_markdown_path = output_root / "latest.md"
    run_log_path = output_root / "run.log"

    payload = build_operator_brief_notification_approval_gate(
        as_of=as_of,
        data_root=data_root,
        input_preview_file=input_preview_file,
        approval_marker_file=approval_marker_file,
        output_json_path=json_path,
        output_markdown_path=markdown_path,
        latest_json_path=latest_json_path,
        latest_markdown_path=latest_markdown_path,
        run_log_path=run_log_path,
        generated_at=generated,
        output_safety_finding=output_safety_finding,
    )
    markdown = render_operator_brief_notification_approval_gate_markdown(payload)
    run_log = render_operator_brief_notification_approval_gate_run_log(payload)
    write_json_atomic(json_path, payload, sort_keys=False)
    write_text_atomic(markdown_path, markdown)
    write_json_atomic(latest_json_path, payload, sort_keys=False)
    write_text_atomic(latest_markdown_path, markdown)
    write_text_atomic(run_log_path, run_log)
    return payload


def build_operator_brief_notification_approval_gate(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    input_preview_file: Path | None = None,
    approval_marker_file: Path | None = None,
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
    output_json_path = output_json_path or default_approval_gate_json_path(data_root, as_of)
    output_markdown_path = output_markdown_path or output_json_path.with_suffix(".md")
    latest_json_path = latest_json_path or default_approval_gate_latest_json_path(data_root)
    latest_markdown_path = latest_markdown_path or default_approval_gate_latest_markdown_path(
        data_root
    )
    run_log_path = run_log_path or default_approval_gate_run_log_path(data_root)

    path_safety_findings: list[str] = []
    if output_safety_finding:
        path_safety_findings.append(output_safety_finding)

    preview_path, preview_path_finding = _resolve_dispatch_preview_path(
        as_of=as_of,
        data_root=data_root,
        project_root=project_root,
        explicit_path=input_preview_file,
    )
    if preview_path_finding:
        path_safety_findings.append(preview_path_finding)
    preview_payload, preview_artifact_status, preview_error = _read_json_object_with_status(
        preview_path,
        unsafe=bool(preview_path_finding),
    )

    marker_path, marker_path_finding = _resolve_approval_marker_path(
        as_of=as_of,
        data_root=data_root,
        project_root=project_root,
        explicit_path=approval_marker_file,
    )
    if marker_path_finding:
        path_safety_findings.append(marker_path_finding)
    marker_payload, marker_status, marker_error = _read_json_object_with_status(
        marker_path,
        unsafe=bool(marker_path_finding),
    )

    reasons: list[str] = []
    warnings: list[str] = []
    if preview_error:
        reasons.append(preview_error)
    if marker_error and marker_status != STATUS_MISSING:
        reasons.append(marker_error)
    warnings.extend(path_safety_findings)

    preview_schema_valid = _dispatch_preview_schema_valid(
        preview_payload=preview_payload,
        artifact_status=preview_artifact_status,
    )
    if preview_artifact_status == STATUS_FOUND and not preview_schema_valid:
        reasons.append("TRADING-032 dispatch preview artifact has an unexpected schema.")

    dispatch_preview_hash = ""
    hash_error = ""
    if preview_schema_valid:
        try:
            dispatch_preview_hash = compute_dispatch_preview_hash(preview_payload)
        except (TypeError, ValueError, OSError) as exc:
            hash_error = f"Dispatch preview hash calculation failed: {exc}"
            reasons.append(hash_error)

    preview_final_status = _dispatch_preview_final_status(preview_payload)
    preview_summary = _dispatch_preview_summary(
        preview_payload=preview_payload,
        final_status=preview_final_status,
    )
    marker_summary, marker_reasons, marker_warnings = _approval_marker_summary(
        marker_payload=marker_payload,
        marker_status=marker_status,
        current_preview_hash=dispatch_preview_hash,
        now=generated,
    )
    reasons.extend(marker_reasons)
    warnings.extend(marker_warnings)

    safety_blocked_from_preview = _preview_has_safety_block(preview_payload, preview_final_status)
    if safety_blocked_from_preview and marker_summary["approved"] is True:
        reasons.append("Approval marker cannot override TRADING-032 safety block.")

    approval_gate_status = _approval_gate_status(
        preview_artifact_status=preview_artifact_status,
        preview_schema_valid=preview_schema_valid,
        preview_final_status=preview_final_status,
        hash_error=hash_error,
        marker_status=marker_status,
        marker_summary=marker_summary,
        path_safety_findings=path_safety_findings,
        safety_blocked_from_preview=safety_blocked_from_preview,
    )
    reasons.extend(
        _decision_reasons(
            approval_gate_status=approval_gate_status,
            preview_final_status=preview_final_status,
            marker_summary=marker_summary,
            preview_schema_valid=preview_schema_valid,
            preview_artifact_status=preview_artifact_status,
            marker_status=marker_status,
        )
    )
    reasons = _unique_strings(reasons)
    warnings = _unique_strings(warnings)

    allowed_to_enter_dispatch = approval_gate_status == APPROVED
    decision = {
        "approval_gate_status": approval_gate_status,
        "allowed_to_enter_dispatch": allowed_to_enter_dispatch,
        "human_action_required": approval_gate_status
        in {
            APPROVAL_REQUIRED,
            APPROVAL_EXPIRED,
            APPROVAL_MISMATCH,
            SAFETY_BLOCKED,
            BLOCKED,
        },
        "next_recommended_action": _next_recommended_action(
            approval_gate_status=approval_gate_status
        ),
    }

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "approval_gate_only": True,
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
        "operator_brief_executed_by_approval_gate": False,
        "notification_draft_executed_by_approval_gate": False,
        "delivery_preflight_executed_by_approval_gate": False,
        "dispatch_preview_executed_by_approval_gate": False,
        "pipelines_executed_by_approval_gate": False,
        "data_downloaded_by_approval_gate": False,
        "apply_executed_by_approval_gate": False,
        "rollback_executed_by_approval_gate": False,
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
            "dispatch_preview_artifact": _input_ref(preview_path, preview_artifact_status),
            "approval_marker": _input_ref(marker_path, marker_status),
        },
        "dispatch_preview_summary": preview_summary,
        "approval_marker_summary": marker_summary,
        "hashes": {
            "dispatch_preview_hash": dispatch_preview_hash,
            "hash_algorithm": HASH_ALGORITHM,
            "hash_scope": HASH_SCOPE,
        },
        "decision": decision,
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "approval_required_for_would_send": True,
            "safety_blocked_from_preview": safety_blocked_from_preview,
            "path_safety_findings": path_safety_findings,
        },
        "reasons": reasons,
        "warnings": warnings,
        "audit": {
            "created_by": "scripts/run_operator_brief_notification_approval_gate.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_approval_gate_artifacts": True,
        },
        "output_artifacts": {
            "approval_gate_json": {"path": str(output_json_path)},
            "approval_gate_markdown": {"path": str(output_markdown_path)},
            "latest_json": {"path": str(latest_json_path)},
            "latest_markdown": {"path": str(latest_markdown_path)},
            "run_log": {"path": str(run_log_path)},
        },
    }
    _assert_approval_gate_safety_invariants(payload)
    return payload


def compute_dispatch_preview_hash(payload: dict[str, Any]) -> str:
    canonical_payload = _canonical_dispatch_preview_payload(payload)
    canonical_json = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return f"{HASH_ALGORITHM}:{digest}"


def render_operator_brief_notification_approval_gate_markdown(
    payload: dict[str, Any],
) -> str:
    metadata = _mapping(payload.get("metadata"))
    preview = _mapping(payload.get("dispatch_preview_summary"))
    marker = _mapping(payload.get("approval_marker_summary"))
    hashes = _mapping(payload.get("hashes"))
    decision = _mapping(payload.get("decision"))
    safety = _mapping(payload.get("safety"))
    lines = [
        "# Operator Brief Notification Approval Gate",
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
        f"- Approval gate status: `{decision.get('approval_gate_status', BLOCKED)}`",
        "- Allowed to enter dispatch: "
        f"`{_bool_text(decision.get('allowed_to_enter_dispatch'))}`",
        f"- Human action required: `{_bool_text(decision.get('human_action_required'))}`",
        f"- Next recommended action: {decision.get('next_recommended_action') or ''}",
        "",
        "## Dispatch Preview Summary",
        "",
        f"- Dispatch preview status: `{preview.get('final_status', BLOCKED)}`",
        f"- Channel count: `{preview.get('channel_count', 0)}`",
        f"- Would-send channel count: `{preview.get('would_send_channel_count', 0)}`",
        "- Human action required from preview: "
        f"`{_bool_text(preview.get('human_action_required'))}`",
        "",
        "## Approval Marker",
        "",
        f"- Exists: `{_bool_text(marker.get('exists'))}`",
        f"- Approved: `{_bool_text(marker.get('approved'))}`",
        f"- Approved by: `{marker.get('approved_by') or ''}`",
        f"- Approved at: `{marker.get('approved_at') or ''}`",
        f"- Expires at: `{marker.get('expires_at') or ''}`",
        f"- Hash matches: `{_bool_text(marker.get('hash_matches'))}`",
        f"- Expired: `{_bool_text(marker.get('expired'))}`",
        "",
        "## Hash",
        "",
        f"- Algorithm: `{hashes.get('hash_algorithm', HASH_ALGORITHM)}`",
        f"- Scope: `{hashes.get('hash_scope', HASH_SCOPE)}`",
        f"- Preview hash: `{hashes.get('dispatch_preview_hash') or ''}`",
        "",
        "## Reasons",
        "",
        *_markdown_list(_strings(payload.get("reasons"))),
        "",
        "## Warnings",
        "",
        *_markdown_list(_strings(payload.get("warnings"))),
        "",
        "## Safety",
        "",
        f"- External side effects: `{_bool_text(safety.get('external_side_effects'))}`",
        f"- Network access required: `{_bool_text(safety.get('network_access_required'))}`",
        f"- Secrets required: `{_bool_text(safety.get('secrets_required'))}`",
        "- Approval required for WOULD_SEND: "
        f"`{_bool_text(safety.get('approval_required_for_would_send'))}`",
        "",
    ]
    return "\n".join(lines)


def render_operator_brief_notification_approval_gate_run_log(
    payload: dict[str, Any],
) -> str:
    decision = _mapping(payload.get("decision"))
    preview = _mapping(payload.get("dispatch_preview_summary"))
    marker = _mapping(payload.get("approval_marker_summary"))
    hashes = _mapping(payload.get("hashes"))
    outputs = _mapping(payload.get("output_artifacts"))
    return "\n".join(
        [
            f"Operator Brief Notification Approval Gate Run - {payload.get('date')}",
            "run_status=COMPLETED",
            f"approval_gate_status={decision.get('approval_gate_status')}",
            f"allowed_to_enter_dispatch={_bool_text(decision.get('allowed_to_enter_dispatch'))}",
            f"dispatch_preview_status={preview.get('final_status')}",
            f"approval_marker_exists={_bool_text(marker.get('exists'))}",
            f"hash_matches={_bool_text(marker.get('hash_matches'))}",
            f"expired={_bool_text(marker.get('expired'))}",
            f"dispatch_preview_hash={hashes.get('dispatch_preview_hash')}",
            f"production_effect={payload.get('production_effect')}",
            f"manual_review_only={_bool_text(payload.get('manual_review_only'))}",
            f"approval_gate_only={_bool_text(payload.get('approval_gate_only'))}",
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
            f"approval_gate_json={_path_from_ref(outputs.get('approval_gate_json'))}",
            f"approval_gate_markdown={_path_from_ref(outputs.get('approval_gate_markdown'))}",
            f"latest_json={_path_from_ref(outputs.get('latest_json'))}",
            f"latest_markdown={_path_from_ref(outputs.get('latest_markdown'))}",
            f"run_log={_path_from_ref(outputs.get('run_log'))}",
            "",
        ]
    )


def should_fail_cli(payload: dict[str, Any]) -> bool:
    return payload.get("runtime_error") is True


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
    default_path = preview_root / f"operator_brief_notification_dispatch_preview_{as_of}.json"
    latest_path = preview_root / "latest.json"
    if latest_path.exists():
        return latest_path, None
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


def _resolve_approval_marker_path(
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
            purpose="approval marker",
        )
    marker_path = default_approval_marker_path(data_root)
    if marker_path.exists():
        return marker_path, None
    approvals_root = default_approval_gate_root(data_root) / "approvals"
    default_path = approvals_root / f"operator_brief_notification_approval_{as_of}.json"
    return (
        _latest_dated_or_latest(
            root=approvals_root,
            prefix="operator_brief_notification_approval_",
            as_of=as_of,
            default_path=marker_path if not approvals_root.exists() else default_path,
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
        return default_approval_gate_root(data_root), None
    path, finding = _resolve_repo_path(
        value=output_dir,
        data_root=data_root,
        project_root=project_root,
        purpose="approval gate output directory",
        allow_missing=True,
    )
    if finding:
        return default_approval_gate_root(data_root), finding
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


def _dispatch_preview_final_status(preview_payload: dict[str, Any]) -> str:
    decision_status = _string_value(_mapping(preview_payload.get("decision")).get("final_status"))
    if decision_status:
        return decision_status
    return _string_value(_mapping(preview_payload.get("dispatch_preview")).get("dispatch_status"))


def _dispatch_preview_summary(
    *,
    preview_payload: dict[str, Any],
    final_status: str,
) -> dict[str, Any]:
    preview = _mapping(preview_payload.get("dispatch_preview"))
    decision = _mapping(preview_payload.get("decision"))
    channels = _records(preview.get("channels"))
    return {
        "final_status": final_status or BLOCKED,
        "human_action_required": decision.get("human_action_required") is True,
        "channel_count": len(channels),
        "would_send_channel_count": sum(
            1 for channel in channels if channel.get("would_send") is True
        ),
    }


def _approval_marker_summary(
    *,
    marker_payload: dict[str, Any],
    marker_status: str,
    current_preview_hash: str,
    now: datetime,
) -> tuple[dict[str, Any], list[str], list[str]]:
    reasons: list[str] = []
    warnings: list[str] = []
    exists = marker_status in {STATUS_FOUND, STATUS_INVALID, STATUS_UNSAFE}
    approved = marker_payload.get("approved") is True if marker_status == STATUS_FOUND else False
    preview_hash = _string_value(marker_payload.get("preview_hash"))
    hash_matches = bool(
        marker_status == STATUS_FOUND
        and current_preview_hash
        and preview_hash
        and preview_hash == current_preview_hash
    )
    expires_at = _string_value(marker_payload.get("expires_at"))
    expires_datetime = _parse_datetime(expires_at)
    expired = bool(expires_datetime is not None and expires_datetime < _normalize_datetime(now))
    approved_by = _mask_email(_string_value(marker_payload.get("approved_by")))
    approved_at = _string_value(marker_payload.get("approved_at"))
    approved_datetime = _parse_datetime(approved_at)
    approval_note = _mask_email(_string_value(marker_payload.get("approval_note")))

    if marker_status == STATUS_FOUND:
        required_fields = ("approved", "approved_by", "approved_at", "expires_at", "preview_hash")
        missing_fields = [field for field in required_fields if field not in marker_payload]
        if missing_fields:
            reasons.append(f"Approval marker is missing required fields: {missing_fields}.")
        if expires_at and expires_datetime is None:
            reasons.append("Approval marker expires_at is not a valid ISO-8601 datetime.")
        if approved_at and approved_datetime is None:
            warnings.append("Approval marker approved_at is not a valid ISO-8601 datetime.")
        if _string_value(marker_payload.get("approval_type")) not in {"", "manual"}:
            warnings.append("Approval marker approval_type is not manual.")

    summary = {
        "exists": exists,
        "approved": approved,
        "approved_by": approved_by,
        "approved_at": approved_at,
        "expires_at": expires_at,
        "approved_at_valid": bool(approved_at and approved_datetime is not None),
        "expires_at_valid": bool(expires_at and expires_datetime is not None),
        "preview_hash": preview_hash,
        "hash_matches": hash_matches,
        "expired": expired,
        "approval_note": approval_note,
    }
    return summary, reasons, warnings


def _preview_has_safety_block(preview_payload: dict[str, Any], final_status: str) -> bool:
    if final_status == DISPATCH_SAFETY_BLOCKED:
        return True
    preview = _mapping(preview_payload.get("dispatch_preview"))
    if _string_value(preview.get("dispatch_status")) == DISPATCH_SAFETY_BLOCKED:
        return True
    safety = _mapping(preview_payload.get("safety"))
    if safety.get("external_side_effects") is True:
        return True
    if safety.get("network_access_required") is True:
        return True
    if safety.get("secrets_required") is True:
        return True
    return bool(_strings(safety.get("sensitive_content_flags")))


def _approval_gate_status(
    *,
    preview_artifact_status: str,
    preview_schema_valid: bool,
    preview_final_status: str,
    hash_error: str,
    marker_status: str,
    marker_summary: dict[str, Any],
    path_safety_findings: list[str],
    safety_blocked_from_preview: bool,
) -> str:
    if path_safety_findings and (
        preview_artifact_status == STATUS_UNSAFE or marker_status == STATUS_UNSAFE
    ):
        return SAFETY_BLOCKED
    if not preview_schema_valid or preview_artifact_status != STATUS_FOUND:
        return BLOCKED
    if hash_error:
        return BLOCKED
    if safety_blocked_from_preview:
        return SAFETY_BLOCKED
    if marker_status == STATUS_INVALID:
        return BLOCKED
    if preview_final_status == DISPATCH_NOOP:
        return NOOP
    if preview_final_status == DISPATCH_BLOCKED:
        return BLOCKED
    if preview_final_status == DISPATCH_NEEDS_APPROVAL:
        return APPROVAL_REQUIRED
    if preview_final_status != DISPATCH_WOULD_SEND:
        return BLOCKED
    if marker_status == STATUS_MISSING:
        return APPROVAL_REQUIRED
    if marker_summary.get("approved") is not True:
        return APPROVAL_REQUIRED
    if not _marker_required_fields_present(marker_summary):
        return APPROVAL_REQUIRED
    if marker_summary.get("hash_matches") is not True:
        return APPROVAL_MISMATCH
    if marker_summary.get("expired") is True:
        return APPROVAL_EXPIRED
    return APPROVED


def _marker_required_fields_present(marker_summary: dict[str, Any]) -> bool:
    return (
        bool(marker_summary.get("approved_by"))
        and bool(marker_summary.get("approved_at"))
        and bool(marker_summary.get("expires_at"))
        and bool(marker_summary.get("preview_hash"))
        and marker_summary.get("approved_at_valid") is True
        and marker_summary.get("expires_at_valid") is True
    )


def _decision_reasons(
    *,
    approval_gate_status: str,
    preview_final_status: str,
    marker_summary: dict[str, Any],
    preview_schema_valid: bool,
    preview_artifact_status: str,
    marker_status: str,
) -> list[str]:
    if approval_gate_status == APPROVED:
        return ["Approval marker is valid for the current TRADING-032 dispatch preview hash."]
    if approval_gate_status == APPROVAL_REQUIRED:
        if preview_final_status == DISPATCH_NEEDS_APPROVAL:
            return ["TRADING-032 dispatch preview still requires approval before dispatch."]
        if marker_status == STATUS_MISSING:
            return ["Approval marker is missing for a WOULD_SEND dispatch preview."]
        if marker_summary.get("approved") is not True:
            return ["Approval marker is absent or approved is not true."]
        return ["Approval marker is missing required approval metadata."]
    if approval_gate_status == APPROVAL_EXPIRED:
        return ["Approval marker matched the preview hash but has expired."]
    if approval_gate_status == APPROVAL_MISMATCH:
        return ["Approval marker preview_hash does not match the current dispatch preview hash."]
    if approval_gate_status == SAFETY_BLOCKED:
        return ["TRADING-032 dispatch preview is safety blocked; approval cannot override it."]
    if approval_gate_status == NOOP:
        return ["TRADING-032 dispatch preview is NOOP; no approval is required."]
    if preview_artifact_status != STATUS_FOUND or not preview_schema_valid:
        return ["TRADING-032 dispatch preview artifact is missing, invalid, or not trusted."]
    if marker_status == STATUS_INVALID:
        return ["Approval marker JSON is invalid."]
    return ["TRADING-032 dispatch preview is blocked or cannot enter dispatch."]


def _next_recommended_action(*, approval_gate_status: str) -> str:
    if approval_gate_status == APPROVED:
        return (
            "Future real dispatch may read this approval gate artifact; "
            "TRADING-033 sent nothing."
        )
    if approval_gate_status == APPROVAL_REQUIRED:
        return "Create or update a manual approval marker for the current dispatch preview hash."
    if approval_gate_status == APPROVAL_EXPIRED:
        return "Re-review the dispatch preview and create a fresh approval marker."
    if approval_gate_status == APPROVAL_MISMATCH:
        return "Re-review the updated dispatch preview and approve the new preview hash."
    if approval_gate_status == SAFETY_BLOCKED:
        return "Resolve TRADING-032 safety findings before any dispatch approval."
    if approval_gate_status == NOOP:
        return "No dispatch approval is needed for this run."
    return "Restore valid TRADING-032 preview and marker inputs before dispatch approval."


def _canonical_dispatch_preview_payload(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _canonical_dispatch_preview_payload(child)
            for key, child in sorted(value.items())
            if key not in HASH_VOLATILE_KEYS
        }
    if isinstance(value, list):
        return [_canonical_dispatch_preview_payload(child) for child in value]
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


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _normalize_datetime(parsed)


def _normalize_datetime(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _mask_email(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        email = match.group(0)
        local, domain = email.split("@", 1)
        prefix = local[:1] if local else "x"
        return f"{prefix}***@{domain}"

    return EMAIL_RE.sub(replace, value)


def _assert_approval_gate_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-033 production_effect must be none")
    for field in SAFETY_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-033 {field} must be true")
    for field in SAFETY_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-033 {field} must be false")
    serialized = json.dumps(payload, ensure_ascii=False)
    forbidden_tokens = ("sent_at", "gmail_token", "api_key=", "password=")
    if any(token in serialized.lower() for token in forbidden_tokens):
        raise ValueError("TRADING-033 must not emit dispatch side effects or secrets")
