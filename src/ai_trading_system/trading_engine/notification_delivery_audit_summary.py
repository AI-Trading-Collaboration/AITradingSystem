from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import sha256_path, write_json_atomic, write_text_atomic

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "notification_delivery_audit_summary"
RUN_REPORT_TYPE = "notification_delivery_audit_summary_run"
TASK_ID = "TRADING-035"
MODE = "notification_delivery_audit_summary_only"
PRODUCTION_EFFECT_NONE = "none"

AUDIT_PASS = "PASS"
AUDIT_PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"
AUDIT_INCOMPLETE = "INCOMPLETE"
AUDIT_MISMATCH = "MISMATCH"
AUDIT_SAFETY_BLOCKED = "SAFETY_BLOCKED"
AUDIT_ERROR = "ERROR"

LIFECYCLE_DRAFT_ONLY = "DRAFT_ONLY"
LIFECYCLE_PREFLIGHT_READY = "PREFLIGHT_READY"
LIFECYCLE_DRAFT_READY = "DRAFT_READY"
LIFECYCLE_BLOCKED = "BLOCKED"
LIFECYCLE_APPROVAL_MISMATCH = "APPROVAL_MISMATCH"
LIFECYCLE_SAFETY_BLOCKED = "SAFETY_BLOCKED"
LIFECYCLE_INCOMPLETE = "INCOMPLETE"
LIFECYCLE_UNKNOWN = "UNKNOWN"

STATUS_FOUND = "FOUND"
STATUS_MISSING = "MISSING"
STATUS_INVALID = "INVALID"

EXPECTED_DRAFT_TASK_ID = "TRADING-030"
EXPECTED_PREFLIGHT_TASK_ID = "TRADING-031"
EXPECTED_DISPATCH_TASK_ID = "TRADING-034"

SIDE_EFFECT_FIELDS = {
    "email_sent",
    "gmail_draft_created",
    "gmail_draft_modified",
    "slack_sent",
    "discord_sent",
    "webhook_called",
    "mobile_push_sent",
    "smtp_called",
    "telegram_sent",
    "external_side_effects",
}
EXECUTION_FIELD_NAMES = {
    "broker_execution",
    "replay_execution",
    "trading_execution",
}
EXECUTION_FIELD_PREFIXES = (
    "data_downloaded",
    "pipelines_executed",
    "apply_executed",
    "rollback_executed",
)
DRAFT_HASH_VOLATILE_KEYS = {
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

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

OUTPUT_SAFETY_TRUE_FIELDS = (
    "manual_review_only",
    "notification_delivery_audit_only",
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
    "notification_draft_executed_by_audit",
    "delivery_preflight_executed_by_audit",
    "draft_dispatch_executed_by_audit",
    "operator_brief_executed_by_audit",
    "pipelines_executed_by_audit",
    "data_downloaded_by_audit",
    "apply_executed_by_audit",
    "rollback_executed_by_audit",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)


def default_notification_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs" / "notifications"


def default_delivery_audit_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "delivery_audit"


def default_delivery_audit_json_path(data_root: Path, as_of: date) -> Path:
    return default_delivery_audit_root(data_root) / (
        f"notification_delivery_audit_summary_{as_of.isoformat()}.json"
    )


def default_delivery_audit_markdown_path(data_root: Path, as_of: date) -> Path:
    return default_delivery_audit_json_path(data_root, as_of).with_suffix(".md")


def default_delivery_audit_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_delivery_audit_root(data_root)
        / "logs"
        / f"notification_delivery_audit_summary_run_{as_of.isoformat()}.json"
    )


def default_notification_draft_metadata_path(data_root: Path, as_of: date) -> Path:
    return default_notification_root(data_root) / (
        f"operator_brief_notification_draft_{as_of.isoformat()}.json"
    )


def default_delivery_preflight_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_notification_root(data_root)
        / "delivery_preflight"
        / f"operator_brief_notification_delivery_preflight_{as_of.isoformat()}.json"
    )


def default_draft_dispatch_root(data_root: Path) -> Path:
    return default_notification_root(data_root) / "draft_dispatch"


def default_draft_dispatch_json_path(data_root: Path, as_of: date) -> Path:
    return default_draft_dispatch_root(data_root) / (
        f"operator_brief_notification_draft_dispatch_{as_of.isoformat()}.json"
    )


def default_draft_dispatch_latest_json_path(data_root: Path) -> Path:
    return default_draft_dispatch_root(data_root) / "latest.json"


def write_notification_delivery_audit_summary(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    notification_draft_metadata_file: Path | None = None,
    delivery_preflight_file: Path | None = None,
    dispatch_latest_file: Path | None = None,
    dispatch_file: Path | None = None,
    allow_missing_dispatch: bool = False,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = _normalize_data_root(data_root)
    output_json_path = default_delivery_audit_json_path(data_root, as_of)
    output_markdown_path = output_json_path.with_suffix(".md")
    run_log_json_path = default_delivery_audit_run_log_json_path(data_root, as_of)
    run_log_markdown_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_notification_delivery_audit_summary(
            as_of=as_of,
            data_root=data_root,
            notification_draft_metadata_file=notification_draft_metadata_file,
            delivery_preflight_file=delivery_preflight_file,
            dispatch_latest_file=dispatch_latest_file,
            dispatch_file=dispatch_file,
            allow_missing_dispatch=allow_missing_dispatch,
            output_json_path=output_json_path,
            output_markdown_path=output_markdown_path,
            run_log_json_path=run_log_json_path,
            run_log_markdown_path=run_log_markdown_path,
            generated_at=generated,
        )
    except Exception as exc:  # pragma: no cover - defensive artifact path
        payload = _error_payload(
            as_of=as_of,
            output_json_path=output_json_path,
            output_markdown_path=output_markdown_path,
            run_log_json_path=run_log_json_path,
            run_log_markdown_path=run_log_markdown_path,
            generated_at=generated,
            error=str(exc),
        )

    write_json_atomic(output_json_path, payload, sort_keys=False)
    write_text_atomic(output_markdown_path, render_notification_delivery_audit_markdown(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    write_json_atomic(run_log_json_path, run_log, sort_keys=False)
    write_text_atomic(run_log_markdown_path, render_notification_delivery_audit_run_log(run_log))
    return payload


def build_notification_delivery_audit_summary(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    notification_draft_metadata_file: Path | None = None,
    delivery_preflight_file: Path | None = None,
    dispatch_latest_file: Path | None = None,
    dispatch_file: Path | None = None,
    allow_missing_dispatch: bool = False,
    output_json_path: Path | None = None,
    output_markdown_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_markdown_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    data_root = _normalize_data_root(data_root)
    output_json_path = output_json_path or default_delivery_audit_json_path(data_root, as_of)
    output_markdown_path = output_markdown_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_delivery_audit_run_log_json_path(
        data_root, as_of
    )
    run_log_markdown_path = run_log_markdown_path or run_log_json_path.with_suffix(".md")

    draft_path = _resolve_latest_or_explicit(
        as_of=as_of,
        data_root=data_root,
        explicit_path=notification_draft_metadata_file,
        root=default_notification_root(data_root),
        prefix="operator_brief_notification_draft_",
        default_path=default_notification_draft_metadata_path(data_root, as_of),
    )
    preflight_path = _resolve_latest_or_explicit(
        as_of=as_of,
        data_root=data_root,
        explicit_path=delivery_preflight_file,
        root=default_notification_root(data_root) / "delivery_preflight",
        prefix="operator_brief_notification_delivery_preflight_",
        default_path=default_delivery_preflight_json_path(data_root, as_of),
    )
    latest_path = (
        _resolve_input_path(data_root, dispatch_latest_file)
        if dispatch_latest_file is not None
        else default_draft_dispatch_latest_json_path(data_root)
    )

    draft_payload, draft_status, draft_error = _read_json_object_with_status(draft_path)
    preflight_payload, preflight_status, preflight_error = _read_json_object_with_status(
        preflight_path
    )
    latest_payload, latest_status, latest_error = _read_json_object_with_status(latest_path)
    dispatch_path = _resolve_dispatch_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=dispatch_file,
        latest_payload=latest_payload,
        latest_status=latest_status,
    )
    dispatch_payload, dispatch_status, dispatch_error = _read_json_object_with_status(dispatch_path)

    artifact_records = {
        "notification_draft_metadata": _artifact_record(draft_path, draft_status, draft_error),
        "delivery_preflight": _artifact_record(preflight_path, preflight_status, preflight_error),
        "dispatch_latest": _artifact_record(latest_path, latest_status, latest_error),
        "dispatch_artifact": _artifact_record(dispatch_path, dispatch_status, dispatch_error),
    }

    chain = _artifact_chain(
        as_of=as_of,
        data_root=data_root,
        draft_path=draft_path,
        draft_payload=draft_payload,
        draft_status=draft_status,
        preflight_path=preflight_path,
        preflight_payload=preflight_payload,
        preflight_status=preflight_status,
        latest_path=latest_path,
        latest_payload=latest_payload,
        latest_status=latest_status,
        dispatch_path=dispatch_path,
        dispatch_payload=dispatch_payload,
        dispatch_status=dispatch_status,
        dispatch_file_explicit=dispatch_file is not None,
        allow_missing_dispatch=allow_missing_dispatch,
    )
    side_effect_audit = _external_side_effect_audit(
        artifacts=(
            ("TRADING-030 notification draft metadata", draft_payload, draft_status),
            ("TRADING-031 delivery preflight", preflight_payload, preflight_status),
            ("TRADING-034 dispatch latest", latest_payload, latest_status),
            ("TRADING-034 dispatch artifact", dispatch_payload, dispatch_status),
        )
    )
    safety = _safety_validation(
        draft_payload=draft_payload,
        draft_status=draft_status,
        preflight_payload=preflight_payload,
        preflight_status=preflight_status,
        dispatch_payload=dispatch_payload,
        dispatch_status=dispatch_status,
        latest_payload=latest_payload,
        latest_status=latest_status,
        side_effect_audit=side_effect_audit,
    )
    invalid_reasons = [
        reason
        for reason in (draft_error, preflight_error, latest_error, dispatch_error)
        if reason and "invalid" in reason.lower()
    ]
    audit_status = _audit_status(
        draft_status=draft_status,
        preflight_status=preflight_status,
        dispatch_status=dispatch_status,
        latest_status=latest_status,
        allow_missing_dispatch=allow_missing_dispatch,
        dispatch_file_explicit=dispatch_file is not None,
        invalid_reasons=invalid_reasons,
        chain=chain,
        safety=safety,
        side_effect_audit=side_effect_audit,
        preflight_payload=preflight_payload,
        dispatch_payload=dispatch_payload,
    )
    lifecycle_status = _notification_lifecycle_status(
        audit_status=audit_status,
        draft_status=draft_status,
        preflight_status=preflight_status,
        dispatch_status=dispatch_status,
        preflight_payload=preflight_payload,
        dispatch_payload=dispatch_payload,
        chain=chain,
    )
    alerts = _alerts(
        audit_status=audit_status,
        invalid_reasons=invalid_reasons,
        chain=chain,
        safety=safety,
        side_effect_audit=side_effect_audit,
    )
    summary_level = _summary_level(audit_status, lifecycle_status, alerts)
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_delivery_audit_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "notification_draft_executed_by_audit": False,
        "delivery_preflight_executed_by_audit": False,
        "draft_dispatch_executed_by_audit": False,
        "operator_brief_executed_by_audit": False,
        "pipelines_executed_by_audit": False,
        "data_downloaded_by_audit": False,
        "apply_executed_by_audit": False,
        "rollback_executed_by_audit": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "audit_status": audit_status,
        "notification_lifecycle_status": lifecycle_status,
        "summary_level": summary_level,
        "headline": _headline(audit_status, lifecycle_status),
        "input_artifacts": artifact_records,
        "artifact_chain": chain,
        "draft_summary": _draft_summary(
            data_root=data_root,
            draft_payload=draft_payload,
            draft_status=draft_status,
        ),
        "preflight_summary": _preflight_summary(
            preflight_payload=preflight_payload,
            preflight_status=preflight_status,
        ),
        "dispatch_summary": _dispatch_summary(
            dispatch_payload=dispatch_payload,
            dispatch_status=dispatch_status,
            chain=chain,
        ),
        "external_side_effect_audit": side_effect_audit,
        "safety_validation": safety,
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(audit_status, lifecycle_status),
        "manual_review_required": {
            "required": True,
            "instructions": [
                "Review notification draft content.",
                "Review delivery preflight status.",
                "Review dispatch hash consistency.",
                "Confirm no external delivery side effects occurred.",
            ],
        },
        "audit": {
            "created_by": "scripts/run_notification_delivery_audit_summary.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_delivery_audit_artifacts": True,
        },
        "output_artifacts": {
            "audit_json": {"path": str(output_json_path)},
            "audit_markdown": {"path": str(output_markdown_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_markdown_path)},
        },
    }
    _assert_audit_safety_invariants(payload)
    return payload


def render_notification_delivery_audit_markdown(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    chain = _mapping(payload.get("artifact_chain"))
    draft = _mapping(payload.get("draft_summary"))
    preflight = _mapping(payload.get("preflight_summary"))
    dispatch = _mapping(payload.get("dispatch_summary"))
    side_effects = _mapping(payload.get("external_side_effect_audit"))
    alerts = _mapping(payload.get("alerts"))
    lines = [
        f"# Notification Delivery Audit Summary - {payload.get('date')}",
        "",
    ]
    if payload.get("audit_status") == AUDIT_SAFETY_BLOCKED:
        lines.extend(["## Notification Delivery Audit Safety Blocked", ""])
    if payload.get("audit_status") == AUDIT_MISMATCH:
        lines.extend(["## Notification Delivery Audit Mismatch", ""])
    if payload.get("audit_status") == AUDIT_INCOMPLETE:
        lines.extend(["## Notification Delivery Audit Incomplete", ""])
    lines.extend(
        [
            "## 1. Audit Summary",
            "",
            f"- Audit Status: `{payload.get('audit_status')}`",
            "- Notification Lifecycle Status: " f"`{payload.get('notification_lifecycle_status')}`",
            f"- Summary Level: `{payload.get('summary_level')}`",
            f"- Manual Review Only: `{_bool_text(payload.get('manual_review_only'))}`",
            f"- Read Only: `{_bool_text(payload.get('read_only'))}`",
            f"- Email Sent: `{_bool_text(payload.get('email_sent'))}`",
            f"- Gmail Draft Created: `{_bool_text(payload.get('gmail_draft_created'))}`",
            f"- Slack Sent: `{_bool_text(payload.get('slack_sent'))}`",
            f"- Discord Sent: `{_bool_text(payload.get('discord_sent'))}`",
            f"- Webhook Called: `{_bool_text(payload.get('webhook_called'))}`",
            f"- Mobile Push Sent: `{_bool_text(payload.get('mobile_push_sent'))}`",
            "",
            "## 2. Artifact Chain",
            "",
            "| Stage | Status | Path | SHA256 |",
            "|---|---:|---|---|",
            _artifact_row("030 Notification Draft", artifacts.get("notification_draft_metadata")),
            _artifact_row("031 Delivery Preflight", artifacts.get("delivery_preflight")),
            _artifact_row("034 Dispatch Latest", artifacts.get("dispatch_latest")),
            _artifact_row("034 Dispatch Artifact", artifacts.get("dispatch_artifact")),
            "",
            f"- Artifact Chain Status: `{chain.get('status')}`",
            f"- Draft To Preflight Match: `{_bool_text(chain.get('draft_to_preflight_match'))}`",
            "- Preflight To Dispatch Match: "
            f"`{_bool_text(chain.get('preflight_to_dispatch_match'))}`",
            f"- Latest JSON Match: `{_bool_text(chain.get('dispatch_latest_match'))}`",
            f"- Draft Hash Match: `{_bool_text(chain.get('draft_hash_match'))}`",
            "",
            "## 3. Draft Summary",
            "",
            f"- Draft Status: `{draft.get('draft_status')}`",
            f"- Notification Severity: `{draft.get('notification_severity')}`",
            f"- Email Draft Available: `{_bool_text(draft.get('email_draft_available'))}`",
            f"- Chat Draft Available: `{_bool_text(draft.get('chat_draft_available'))}`",
            "- Mobile Summary Available: " f"`{_bool_text(draft.get('mobile_summary_available'))}`",
            f"- Redaction Warning Count: `{draft.get('redaction_warning_count')}`",
            "",
            "## 4. Preflight Summary",
            "",
            f"- Preflight Status: `{preflight.get('preflight_status')}`",
            f"- Delivery Readiness: `{preflight.get('delivery_readiness')}`",
            f"- Approval Required: `{_bool_text(preflight.get('approval_required'))}`",
            f"- Email Channel: `{preflight.get('channel_readiness', {}).get('email')}`",
            f"- Chat Channel: `{preflight.get('channel_readiness', {}).get('chat')}`",
            f"- Mobile Channel: `{preflight.get('channel_readiness', {}).get('mobile')}`",
            "",
            "## 5. Dispatch Summary",
            "",
            f"- Dispatch Status: `{dispatch.get('dispatch_status')}`",
            f"- Draft Hash Match: `{_bool_text(chain.get('draft_hash_match'))}`",
            f"- Latest JSON Match: `{_bool_text(chain.get('dispatch_latest_match'))}`",
            f"- History Preserved: `{_bool_text(dispatch.get('history_preserved'))}`",
            f"- External Side Effects: `{_bool_text(dispatch.get('external_side_effects'))}`",
            "",
            "## 6. External Side Effect Audit",
            "",
            "| Side Effect | Value |",
            "|---|---:|",
        ]
    )
    for field in (
        "email_sent",
        "gmail_draft_created",
        "gmail_draft_modified",
        "slack_sent",
        "discord_sent",
        "webhook_called",
        "mobile_push_sent",
    ):
        lines.append(f"| {field} | `{_bool_text(side_effects.get(field))}` |")
    lines.extend(
        [
            "",
            "## 7. Alerts",
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
            "## 8. Recommended Next Steps",
            "",
            *_markdown_list(_strings(payload.get("recommended_next_steps"))),
            "",
        ]
    )
    return "\n".join(lines)


def render_notification_delivery_audit_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Notification Delivery Audit Summary Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- audit_status: `{payload.get('audit_status')}`",
            "- notification_lifecycle_status: " f"`{payload.get('notification_lifecycle_status')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- notification_delivery_audit_only: `true`",
            "- read_only: `true`",
            "- email_sent: `false`",
            "- gmail_draft_created: `false`",
            "- gmail_draft_modified: `false`",
            "- slack_sent: `false`",
            "- discord_sent: `false`",
            "- webhook_called: `false`",
            "- mobile_push_sent: `false`",
            "- notification_draft_executed_by_audit: `false`",
            "- delivery_preflight_executed_by_audit: `false`",
            "- draft_dispatch_executed_by_audit: `false`",
            "- operator_brief_executed_by_audit: `false`",
            "- pipelines_executed_by_audit: `false`",
            "- data_downloaded_by_audit: `false`",
            "- apply_executed_by_audit: `false`",
            "- rollback_executed_by_audit: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- audit_json: `{payload.get('audit_json')}`",
            f"- audit_markdown: `{payload.get('audit_markdown')}`",
            "",
        ]
    )


def should_fail_cli(payload: dict[str, Any], *, fail_on_safety_anomaly: bool = False) -> bool:
    return fail_on_safety_anomaly and payload.get("audit_status") in {
        AUDIT_MISMATCH,
        AUDIT_SAFETY_BLOCKED,
        AUDIT_ERROR,
    }


def _artifact_chain(
    *,
    as_of: date,
    data_root: Path,
    draft_path: Path,
    draft_payload: dict[str, Any],
    draft_status: str,
    preflight_path: Path,
    preflight_payload: dict[str, Any],
    preflight_status: str,
    latest_path: Path,
    latest_payload: dict[str, Any],
    latest_status: str,
    dispatch_path: Path,
    dispatch_payload: dict[str, Any],
    dispatch_status: str,
    dispatch_file_explicit: bool,
    allow_missing_dispatch: bool,
) -> dict[str, Any]:
    blocking: list[str] = []
    warnings: list[str] = []
    hash_mismatch = False
    draft_to_preflight_match = True
    preflight_to_dispatch_match = True
    dispatch_latest_match = True
    draft_hash_match = True
    dispatch_hash_stable = True
    latest_json_points_to_dispatch = True

    if draft_status == STATUS_FOUND and preflight_status == STATUS_FOUND:
        draft_ref = _artifact_ref(
            preflight_payload,
            ("input_artifacts", "notification_draft_metadata"),
        )
        if not draft_ref:
            draft_to_preflight_match = False
            blocking.append(
                "TRADING-031 preflight is missing input_artifacts.notification_draft_metadata."
            )
        else:
            if not _same_path(draft_ref.get("path"), draft_path, data_root):
                draft_to_preflight_match = False
                blocking.append(
                    "TRADING-031 preflight references a different TRADING-030 draft path."
                )
            draft_sha = sha256_path(draft_path)
            ref_sha = _string_value(draft_ref.get("sha256"))
            if ref_sha and ref_sha != draft_sha:
                draft_to_preflight_match = False
                draft_hash_match = False
                hash_mismatch = True
                blocking.append(
                    "TRADING-031 preflight references a different TRADING-030 draft hash."
                )
            if not ref_sha:
                warnings.append("TRADING-031 preflight does not expose the draft SHA256.")
        if _string_value(preflight_payload.get("notification_severity")) != _string_value(
            draft_payload.get("notification_severity")
        ):
            draft_to_preflight_match = False
            blocking.append("TRADING-031 notification_severity differs from TRADING-030.")

    if preflight_status == STATUS_FOUND and dispatch_status == STATUS_FOUND:
        preflight_ref = _first_artifact_ref(
            dispatch_payload,
            (
                ("input_artifacts", "delivery_preflight"),
                ("input_artifacts", "notification_delivery_preflight"),
                ("input_refs", "delivery_preflight_artifact"),
                ("input_refs", "preflight_artifact"),
            ),
        )
        if preflight_ref:
            if not _same_path(preflight_ref.get("path"), preflight_path, data_root):
                preflight_to_dispatch_match = False
                blocking.append("TRADING-034 dispatch references a different preflight path.")
            ref_sha = _string_value(preflight_ref.get("sha256"))
            if ref_sha and ref_sha != sha256_path(preflight_path):
                preflight_to_dispatch_match = False
                hash_mismatch = True
                blocking.append("TRADING-034 dispatch references a different preflight hash.")
        draft_ref = _first_artifact_ref(
            dispatch_payload,
            (
                ("input_artifacts", "notification_draft_metadata"),
                ("input_refs", "notification_draft_metadata"),
                ("input_refs", "draft_metadata_artifact"),
            ),
        )
        if draft_ref:
            if not _same_path(draft_ref.get("path"), draft_path, data_root):
                preflight_to_dispatch_match = False
                blocking.append("TRADING-034 dispatch references a different draft metadata path.")
            ref_sha = _string_value(draft_ref.get("sha256"))
            if ref_sha and ref_sha != sha256_path(draft_path):
                preflight_to_dispatch_match = False
                draft_hash_match = False
                hash_mismatch = True
                blocking.append("TRADING-034 dispatch references a different draft metadata hash.")
        if (
            _preflight_is_not_ready(preflight_payload)
            and _dispatch_final_status(dispatch_payload) == "DRAFT_READY"
        ):
            preflight_to_dispatch_match = False
            blocking.append("TRADING-034 is DRAFT_READY while TRADING-031 is not ready.")

        declared_hash = _string_value(_mapping(dispatch_payload.get("hashes")).get("draft_hash"))
        recomputed_hash = _compute_draft_dispatch_hash(dispatch_payload)
        if not declared_hash:
            dispatch_hash_stable = False
            draft_hash_match = False
            hash_mismatch = True
            blocking.append("TRADING-034 dispatch is missing hashes.draft_hash.")
        elif declared_hash != recomputed_hash:
            dispatch_hash_stable = False
            draft_hash_match = False
            preflight_to_dispatch_match = False
            hash_mismatch = True
            blocking.append("TRADING-034 dispatch draft_hash is not stable.")

    if latest_status == STATUS_FOUND:
        if dispatch_status == STATUS_FOUND:
            latest_dispatch_ref = _artifact_ref(
                latest_payload,
                ("output_artifacts", "draft_dispatch_json"),
            )
            if latest_dispatch_ref and latest_dispatch_ref.get("path"):
                latest_json_points_to_dispatch = _same_path(
                    latest_dispatch_ref.get("path"),
                    dispatch_path,
                    data_root,
                )
                if not latest_json_points_to_dispatch:
                    dispatch_latest_match = False
                    blocking.append("TRADING-034 latest.json points to a different dispatch file.")
            else:
                latest_json_points_to_dispatch = False
                dispatch_latest_match = False
                warnings.append(
                    "TRADING-034 latest.json does not expose output_artifacts.draft_dispatch_json."
                )
            if latest_payload.get("date") != dispatch_payload.get("date"):
                dispatch_latest_match = False
                blocking.append("TRADING-034 latest.json date differs from dispatch artifact.")
            if _dispatch_final_status(latest_payload) != _dispatch_final_status(dispatch_payload):
                dispatch_latest_match = False
                blocking.append("TRADING-034 latest.json status differs from dispatch artifact.")
            latest_hash = _string_value(_mapping(latest_payload.get("hashes")).get("draft_hash"))
            dispatch_hash = _string_value(
                _mapping(dispatch_payload.get("hashes")).get("draft_hash")
            )
            if latest_hash != dispatch_hash:
                dispatch_latest_match = False
                hash_mismatch = True
                blocking.append(
                    "TRADING-034 latest.json draft_hash differs from dispatch artifact."
                )
        elif not allow_missing_dispatch:
            dispatch_latest_match = False
            blocking.append("TRADING-034 dispatch artifact is missing.")
    elif dispatch_file_explicit and dispatch_status == STATUS_FOUND:
        dispatch_latest_match = False
        latest_json_points_to_dispatch = False
        warnings.append("TRADING-034 latest.json is missing; explicit dispatch file was audited.")
    elif not allow_missing_dispatch:
        dispatch_latest_match = False
        latest_json_points_to_dispatch = False
        blocking.append("TRADING-034 latest.json is missing.")
    else:
        dispatch_latest_match = False
        latest_json_points_to_dispatch = False
        warnings.append("TRADING-034 dispatch is missing and allow_missing_dispatch=true.")

    if dispatch_status == STATUS_MISSING and allow_missing_dispatch:
        warnings.append("TRADING-034 dispatch artifact is missing by allowed configuration.")
    elif dispatch_status == STATUS_MISSING:
        blocking.append("TRADING-034 dispatch artifact is missing.")

    status = "PASS"
    if blocking:
        status = "FAIL"
    elif warnings:
        status = "PASS_WITH_WARNINGS"

    history_preserved = dispatch_status == STATUS_FOUND and dispatch_path != latest_path
    return {
        "status": status,
        "draft_to_preflight_match": draft_to_preflight_match,
        "preflight_to_dispatch_match": preflight_to_dispatch_match,
        "dispatch_latest_match": dispatch_latest_match,
        "draft_hash_match": draft_hash_match,
        "dispatch_hash_stable": dispatch_hash_stable,
        "latest_json_points_to_dispatch": latest_json_points_to_dispatch,
        "history_preserved": history_preserved,
        "hash_mismatch": hash_mismatch,
        "blocking_reasons": _unique_strings(blocking),
        "warnings": _unique_strings(warnings),
        "date_match": _all_found_dates_match(
            as_of,
            draft_payload,
            preflight_payload,
            dispatch_payload,
        ),
    }


def _draft_summary(
    *,
    data_root: Path,
    draft_payload: dict[str, Any],
    draft_status: str,
) -> dict[str, Any]:
    if draft_status != STATUS_FOUND:
        return {
            "status": "MISSING" if draft_status == STATUS_MISSING else "INVALID",
            "draft_status": "MISSING",
            "notification_severity": "UNKNOWN",
            "email_draft_available": False,
            "chat_draft_available": False,
            "mobile_summary_available": False,
            "redaction_warning_count": 0,
        }
    outputs = _mapping(draft_payload.get("draft_outputs"))
    alerts = _mapping(draft_payload.get("alerts"))
    warnings = _strings(alerts.get("warnings"))
    return {
        "status": "AVAILABLE",
        "draft_status": _string_value(draft_payload.get("draft_status")) or "UNKNOWN",
        "notification_severity": (
            _string_value(draft_payload.get("notification_severity")) or "UNKNOWN"
        ),
        "email_draft_available": _draft_output_exists(data_root, outputs, "email_draft"),
        "chat_draft_available": _draft_output_exists(data_root, outputs, "chat_draft"),
        "mobile_summary_available": _draft_output_exists(data_root, outputs, "mobile_summary"),
        "redaction_warning_count": sum(1 for item in warnings if "redact" in item.lower()),
    }


def _preflight_summary(
    *,
    preflight_payload: dict[str, Any],
    preflight_status: str,
) -> dict[str, Any]:
    if preflight_status != STATUS_FOUND:
        return {
            "status": "MISSING" if preflight_status == STATUS_MISSING else "INVALID",
            "preflight_status": "MISSING",
            "delivery_readiness": "UNKNOWN",
            "approval_required": True,
            "channel_readiness": {"email": "MISSING", "chat": "MISSING", "mobile": "MISSING"},
        }
    approval = _mapping(preflight_payload.get("approval_validation"))
    channels = _mapping(preflight_payload.get("channel_readiness"))
    return {
        "status": _string_value(preflight_payload.get("preflight_status")) or "UNKNOWN",
        "preflight_status": _string_value(preflight_payload.get("preflight_status")) or "UNKNOWN",
        "delivery_readiness": _string_value(preflight_payload.get("delivery_readiness"))
        or "UNKNOWN",
        "approval_required": approval.get("approval_required") is True,
        "channel_readiness": {
            "email": _string_value(_mapping(channels.get("email")).get("status")) or "MISSING",
            "chat": _string_value(_mapping(channels.get("chat")).get("status")) or "MISSING",
            "mobile": _string_value(_mapping(channels.get("mobile")).get("status")) or "MISSING",
        },
    }


def _dispatch_summary(
    *,
    dispatch_payload: dict[str, Any],
    dispatch_status: str,
    chain: dict[str, Any],
) -> dict[str, Any]:
    if dispatch_status != STATUS_FOUND:
        return {
            "status": "MISSING" if dispatch_status == STATUS_MISSING else "INVALID",
            "dispatch_status": "MISSING",
            "draft_hash": "",
            "latest_json_points_to_dispatch": chain.get("latest_json_points_to_dispatch") is True,
            "history_preserved": chain.get("history_preserved") is True,
            "external_side_effects": False,
        }
    hashes = _mapping(dispatch_payload.get("hashes"))
    return {
        "status": _dispatch_final_status(dispatch_payload),
        "dispatch_status": _dispatch_final_status(dispatch_payload),
        "draft_hash": _string_value(hashes.get("draft_hash")),
        "latest_json_points_to_dispatch": chain.get("latest_json_points_to_dispatch") is True,
        "history_preserved": chain.get("history_preserved") is True,
        "external_side_effects": _contains_true_key(dispatch_payload, SIDE_EFFECT_FIELDS),
    }


def _external_side_effect_audit(
    *,
    artifacts: tuple[tuple[str, dict[str, Any], str], ...],
) -> dict[str, Any]:
    detected = {
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
    }
    blocking: list[str] = []
    execution_blocking: list[str] = []
    for label, payload, status in artifacts:
        if status != STATUS_FOUND:
            continue
        for path, key, value in _walk_mapping(payload):
            if value is not True:
                continue
            if key in detected:
                detected[key] = True
            if key in SIDE_EFFECT_FIELDS:
                blocking.append(f"{label} has {path}=true.")
            if _is_execution_field(key):
                execution_blocking.append(f"{label} has {path}=true.")
    all_blocking = _unique_strings([*blocking, *execution_blocking])
    return {
        "status": "FAIL" if all_blocking else "PASS",
        **detected,
        "blocking_reasons": all_blocking,
        "execution_blocking_reasons": _unique_strings(execution_blocking),
    }


def _safety_validation(
    *,
    draft_payload: dict[str, Any],
    draft_status: str,
    preflight_payload: dict[str, Any],
    preflight_status: str,
    dispatch_payload: dict[str, Any],
    dispatch_status: str,
    latest_payload: dict[str, Any],
    latest_status: str,
    side_effect_audit: dict[str, Any],
) -> dict[str, Any]:
    blocking: list[str] = []
    draft_safe = _artifact_safety_ok(
        payload=draft_payload,
        status=draft_status,
        expected_task_id=EXPECTED_DRAFT_TASK_ID,
        required_true=("manual_review_only", "notification_draft_only", "read_only"),
        label="TRADING-030 notification draft metadata",
        blocking=blocking,
    )
    preflight_safe = _artifact_safety_ok(
        payload=preflight_payload,
        status=preflight_status,
        expected_task_id=EXPECTED_PREFLIGHT_TASK_ID,
        required_true=(
            "manual_review_only",
            "notification_delivery_preflight_only",
            "read_only",
        ),
        label="TRADING-031 delivery preflight",
        blocking=blocking,
    )
    dispatch_safe = _artifact_safety_ok(
        payload=dispatch_payload,
        status=dispatch_status,
        expected_task_id=EXPECTED_DISPATCH_TASK_ID,
        required_true=("manual_review_only", "draft_dispatch_only", "read_only"),
        label="TRADING-034 draft dispatch",
        blocking=blocking,
    )
    if latest_status == STATUS_FOUND:
        _artifact_safety_ok(
            payload=latest_payload,
            status=latest_status,
            expected_task_id=EXPECTED_DISPATCH_TASK_ID,
            required_true=("manual_review_only", "draft_dispatch_only", "read_only"),
            label="TRADING-034 latest.json",
            blocking=blocking,
        )
    blocking.extend(_strings(side_effect_audit.get("blocking_reasons")))
    execution_blocking = _strings(side_effect_audit.get("execution_blocking_reasons"))
    no_external_delivery = side_effect_audit.get("status") != "FAIL" or not any(
        key in reason
        for reason in _strings(side_effect_audit.get("blocking_reasons"))
        for key in SIDE_EFFECT_FIELDS
    )
    return {
        "status": "FAIL" if blocking else "PASS",
        "draft_metadata_safe": draft_safe,
        "delivery_preflight_safe": preflight_safe,
        "dispatch_safe": dispatch_safe,
        "no_external_delivery": no_external_delivery,
        "no_pipeline_execution": not execution_blocking,
        "no_data_download": not any("data_downloaded" in item for item in execution_blocking),
        "no_apply_or_rollback": not any(
            "apply_executed" in item or "rollback_executed" in item for item in execution_blocking
        ),
        "no_broker_replay_trading": not any(
            token in item
            for item in execution_blocking
            for token in ("broker_execution", "replay_execution", "trading_execution")
        ),
        "blocking_reasons": _unique_strings(blocking),
    }


def _artifact_safety_ok(
    *,
    payload: dict[str, Any],
    status: str,
    expected_task_id: str,
    required_true: tuple[str, ...],
    label: str,
    blocking: list[str],
) -> bool:
    if status != STATUS_FOUND:
        return False
    safe = True
    if payload.get("task_id") != expected_task_id:
        blocking.append(f"{label} task_id must be {expected_task_id}.")
        safe = False
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        blocking.append(f"{label} production_effect must be none.")
        safe = False
    for field in required_true:
        if payload.get(field) is not True:
            blocking.append(f"{label} must have {field}=true.")
            safe = False
    for path, key, value in _walk_mapping(payload):
        if value is True and key in SIDE_EFFECT_FIELDS:
            blocking.append(f"{label} must not have {path}=true.")
            safe = False
        if value is True and _is_execution_field(key):
            blocking.append(f"{label} must not have {path}=true.")
            safe = False
        if value is True and key in {"network_access_required", "secrets_required"}:
            blocking.append(f"{label} must not have {path}=true.")
            safe = False
    return safe


def _audit_status(
    *,
    draft_status: str,
    preflight_status: str,
    dispatch_status: str,
    latest_status: str,
    allow_missing_dispatch: bool,
    dispatch_file_explicit: bool,
    invalid_reasons: list[str],
    chain: dict[str, Any],
    safety: dict[str, Any],
    side_effect_audit: dict[str, Any],
    preflight_payload: dict[str, Any],
    dispatch_payload: dict[str, Any],
) -> str:
    if invalid_reasons:
        return AUDIT_ERROR
    if safety.get("status") == "FAIL" or side_effect_audit.get("status") == "FAIL":
        return AUDIT_SAFETY_BLOCKED
    if _preflight_status(preflight_payload) == "SAFETY_BLOCKED":
        return AUDIT_SAFETY_BLOCKED
    if _dispatch_final_status(dispatch_payload) == "SAFETY_BLOCKED":
        return AUDIT_SAFETY_BLOCKED
    if draft_status != STATUS_FOUND or preflight_status != STATUS_FOUND:
        return AUDIT_INCOMPLETE
    dispatch_missing = dispatch_status != STATUS_FOUND or latest_status != STATUS_FOUND
    if dispatch_missing and not (allow_missing_dispatch or dispatch_file_explicit):
        return AUDIT_INCOMPLETE
    if chain.get("blocking_reasons"):
        return AUDIT_MISMATCH
    if dispatch_status != STATUS_FOUND and allow_missing_dispatch:
        return AUDIT_PASS_WITH_WARNINGS
    lifecycle_status = _notification_lifecycle_status(
        audit_status=AUDIT_PASS,
        draft_status=draft_status,
        preflight_status=preflight_status,
        dispatch_status=dispatch_status,
        preflight_payload=preflight_payload,
        dispatch_payload=dispatch_payload,
        chain=chain,
    )
    if (
        chain.get("warnings")
        or latest_status != STATUS_FOUND
        or _preflight_status(preflight_payload) != "PASS"
        or lifecycle_status in {LIFECYCLE_BLOCKED, LIFECYCLE_APPROVAL_MISMATCH, LIFECYCLE_UNKNOWN}
    ):
        return AUDIT_PASS_WITH_WARNINGS
    return AUDIT_PASS


def _notification_lifecycle_status(
    *,
    audit_status: str,
    draft_status: str,
    preflight_status: str,
    dispatch_status: str,
    preflight_payload: dict[str, Any],
    dispatch_payload: dict[str, Any],
    chain: dict[str, Any],
) -> str:
    if audit_status in {AUDIT_ERROR}:
        return LIFECYCLE_UNKNOWN
    if audit_status == AUDIT_SAFETY_BLOCKED:
        return LIFECYCLE_SAFETY_BLOCKED
    if chain.get("hash_mismatch") is True:
        return LIFECYCLE_APPROVAL_MISMATCH
    if draft_status != STATUS_FOUND:
        return LIFECYCLE_INCOMPLETE
    if preflight_status != STATUS_FOUND:
        return LIFECYCLE_DRAFT_ONLY
    preflight_value = _preflight_status(preflight_payload)
    if dispatch_status == STATUS_FOUND:
        dispatch_value = _dispatch_final_status(dispatch_payload)
        if dispatch_value == "SAFETY_BLOCKED":
            return LIFECYCLE_SAFETY_BLOCKED
        if dispatch_value == "APPROVAL_MISMATCH":
            return LIFECYCLE_APPROVAL_MISMATCH
        if dispatch_value == "BLOCKED":
            return LIFECYCLE_BLOCKED
        if dispatch_value == "DRAFT_READY" and preflight_value in {"PASS", "PASS_WITH_WARNINGS"}:
            return LIFECYCLE_DRAFT_READY
        if dispatch_value in {"APPROVAL_REQUIRED", "APPROVAL_EXPIRED", "NOOP"}:
            return LIFECYCLE_BLOCKED
    if preflight_value in {"PASS", "PASS_WITH_WARNINGS"}:
        return LIFECYCLE_PREFLIGHT_READY
    if preflight_value == "SAFETY_BLOCKED":
        return LIFECYCLE_SAFETY_BLOCKED
    if preflight_value in {"BLOCKED", "INPUT_MISSING", "INPUT_INVALID"}:
        return LIFECYCLE_BLOCKED
    return LIFECYCLE_UNKNOWN


def _alerts(
    *,
    audit_status: str,
    invalid_reasons: list[str],
    chain: dict[str, Any],
    safety: dict[str, Any],
    side_effect_audit: dict[str, Any],
) -> dict[str, list[str]]:
    critical: list[str] = []
    warnings: list[str] = []
    notes = ["Notification delivery audit is read-only and did not send any notification."]
    if audit_status == AUDIT_ERROR:
        critical.extend(invalid_reasons)
    if audit_status in {AUDIT_MISMATCH, AUDIT_SAFETY_BLOCKED}:
        critical.extend(_strings(chain.get("blocking_reasons")))
        critical.extend(_strings(safety.get("blocking_reasons")))
        critical.extend(_strings(side_effect_audit.get("blocking_reasons")))
    else:
        warnings.extend(_strings(chain.get("blocking_reasons")))
    warnings.extend(_strings(chain.get("warnings")))
    return {
        "critical": _unique_strings(critical),
        "warnings": _unique_strings(warnings),
        "notes": notes,
    }


def _recommended_next_steps(audit_status: str, lifecycle_status: str) -> list[str]:
    if audit_status == AUDIT_SAFETY_BLOCKED:
        return [
            "Stop notification handling until the safety anomaly is resolved.",
            "Review the source artifacts and confirm why a side-effect or execution flag is true.",
            "Do not send any notification from this artifact chain.",
        ]
    if audit_status == AUDIT_MISMATCH:
        return [
            "Review draft, preflight, latest.json, and dispatch artifact hashes.",
            "Do not use the dispatch artifact until the chain mismatch is explained.",
            "Regenerate or re-approve upstream artifacts in separate tasks if needed.",
        ]
    if audit_status == AUDIT_INCOMPLETE:
        return [
            "Restore the missing TRADING-030, TRADING-031, or TRADING-034 artifact.",
            "Do not infer delivery readiness from partial artifacts.",
        ]
    if lifecycle_status == LIFECYCLE_DRAFT_READY:
        return [
            "Review notification drafts manually.",
            (
                "Do not send urgent notifications without checking source operator brief "
                "and approval requirements."
            ),
            (
                "Use this audit as the traceability record before any future delivery "
                "preflight or manual send."
            ),
        ]
    return [
        "Review notification drafts manually.",
        "Confirm delivery preflight and dispatch status before any future send-related task.",
    ]


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("output_artifacts"))
    audit_json = _mapping(outputs.get("audit_json"))
    audit_markdown = _mapping(outputs.get("audit_markdown"))
    run_log_json = _mapping(outputs.get("run_log_json"))
    run_log_markdown = _mapping(outputs.get("run_log_markdown"))
    run_status = "ERROR" if payload.get("audit_status") == AUDIT_ERROR else "COMPLETED"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "created_at": _isoformat_z(generated_at),
        "run_status": run_status,
        "audit_status": payload.get("audit_status"),
        "notification_lifecycle_status": payload.get("notification_lifecycle_status"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "notification_delivery_audit_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "notification_draft_executed_by_audit": False,
        "delivery_preflight_executed_by_audit": False,
        "draft_dispatch_executed_by_audit": False,
        "operator_brief_executed_by_audit": False,
        "pipelines_executed_by_audit": False,
        "data_downloaded_by_audit": False,
        "apply_executed_by_audit": False,
        "rollback_executed_by_audit": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "audit_json": audit_json.get("path"),
        "audit_markdown": audit_markdown.get("path"),
        "run_log_json": run_log_json.get("path"),
        "run_log_markdown": run_log_markdown.get("path"),
    }


def _error_payload(
    *,
    as_of: date,
    output_json_path: Path,
    output_markdown_path: Path,
    run_log_json_path: Path,
    run_log_markdown_path: Path,
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
        "notification_delivery_audit_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "notification_draft_executed_by_audit": False,
        "delivery_preflight_executed_by_audit": False,
        "draft_dispatch_executed_by_audit": False,
        "operator_brief_executed_by_audit": False,
        "pipelines_executed_by_audit": False,
        "data_downloaded_by_audit": False,
        "apply_executed_by_audit": False,
        "rollback_executed_by_audit": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "audit_status": AUDIT_ERROR,
        "notification_lifecycle_status": LIFECYCLE_UNKNOWN,
        "summary_level": "CRITICAL",
        "headline": "Notification delivery audit failed at runtime.",
        "input_artifacts": {},
        "artifact_chain": {
            "status": "FAIL",
            "draft_to_preflight_match": False,
            "preflight_to_dispatch_match": False,
            "dispatch_latest_match": False,
            "draft_hash_match": False,
            "dispatch_hash_stable": False,
            "blocking_reasons": [error],
            "warnings": [],
        },
        "draft_summary": {},
        "preflight_summary": {},
        "dispatch_summary": {},
        "external_side_effect_audit": {
            "status": "PASS",
            "email_sent": False,
            "gmail_draft_created": False,
            "gmail_draft_modified": False,
            "slack_sent": False,
            "discord_sent": False,
            "webhook_called": False,
            "mobile_push_sent": False,
            "blocking_reasons": [],
        },
        "safety_validation": {
            "status": "FAIL",
            "draft_metadata_safe": False,
            "delivery_preflight_safe": False,
            "dispatch_safe": False,
            "no_external_delivery": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": [error],
        },
        "alerts": {"critical": [error], "warnings": [], "notes": []},
        "recommended_next_steps": ["Review the audit run log before using notification artifacts."],
        "manual_review_required": {
            "required": True,
            "instructions": ["Review run log before any notification handling."],
        },
        "audit": {
            "created_by": "scripts/run_notification_delivery_audit_summary.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_delivery_audit_artifacts": True,
        },
        "output_artifacts": {
            "audit_json": {"path": str(output_json_path)},
            "audit_markdown": {"path": str(output_markdown_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_markdown_path)},
        },
    }
    _assert_audit_safety_invariants(payload)
    return payload


def _resolve_latest_or_explicit(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
    root: Path,
    prefix: str,
    default_path: Path,
) -> Path:
    if explicit_path is not None:
        return _resolve_input_path(data_root, explicit_path)
    if not root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*.json"):
        raw_date = path.stem.removeprefix(prefix)
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _resolve_dispatch_path(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
    latest_payload: dict[str, Any],
    latest_status: str,
) -> Path:
    if explicit_path is not None:
        return _resolve_input_path(data_root, explicit_path)
    if latest_status == STATUS_FOUND:
        ref = _artifact_ref(latest_payload, ("output_artifacts", "draft_dispatch_json"))
        if ref and ref.get("path"):
            return _resolve_input_path(data_root, Path(str(ref["path"])))
    return default_draft_dispatch_json_path(data_root, as_of)


def _read_json_object_with_status(path: Path) -> tuple[dict[str, Any], str, str]:
    if not path.exists():
        return {}, STATUS_MISSING, f"JSON input not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, STATUS_INVALID, f"JSON input invalid: {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, STATUS_INVALID, f"JSON input must be an object: {path}"
    return payload, STATUS_FOUND, ""


def _artifact_record(path: Path, status: str, error: str = "") -> dict[str, Any]:
    return {
        "status": status,
        "path": str(path),
        "sha256": sha256_path(path) if path.exists() and path.is_file() else "",
        "error": error,
    }


def _artifact_ref(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    current: object = payload
    for key in keys:
        current = _mapping(current).get(key)
    return _mapping(current)


def _first_artifact_ref(
    payload: dict[str, Any],
    candidates: tuple[tuple[str, ...], ...],
) -> dict[str, Any]:
    for keys in candidates:
        ref = _artifact_ref(payload, keys)
        if ref:
            return ref
    return {}


def _preflight_is_not_ready(payload: dict[str, Any]) -> bool:
    return _preflight_status(payload) not in {"PASS", "PASS_WITH_WARNINGS"}


def _preflight_status(payload: dict[str, Any]) -> str:
    return _string_value(payload.get("preflight_status")) or "UNKNOWN"


def _dispatch_final_status(payload: dict[str, Any]) -> str:
    decision = _mapping(payload.get("decision"))
    draft = _mapping(payload.get("draft"))
    return (
        _string_value(decision.get("final_status"))
        or _string_value(draft.get("draft_status"))
        or "UNKNOWN"
    )


def _draft_output_exists(data_root: Path, outputs: dict[str, Any], key: str) -> bool:
    record = _mapping(outputs.get(key))
    path_text = _string_value(record.get("path"))
    if not path_text:
        return False
    return _resolve_input_path(data_root, Path(path_text)).is_file()


def _same_path(value: object, expected: Path, data_root: Path) -> bool:
    if value is None:
        return False
    actual = _resolve_input_path(data_root, Path(str(value)))
    return _normalized_path_text(actual) == _normalized_path_text(expected)


def _resolve_input_path(data_root: Path, value: Path | str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path.resolve(strict=False)
    project_root = _project_root_from_data_root(data_root)
    candidates = [project_root / path, data_root / path, Path.cwd() / path]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve(strict=False)
    if path.parts and path.parts[0] == data_root.name:
        return (project_root / path).resolve(strict=False)
    return (data_root / path).resolve(strict=False)


def _normalize_data_root(data_root: Path) -> Path:
    value = Path(data_root)
    if value.is_absolute():
        return value.resolve(strict=False)
    return (Path.cwd() / value).resolve(strict=False)


def _project_root_from_data_root(data_root: Path) -> Path:
    return data_root.parent if data_root.name == "data" else REPO_ROOT


def _normalized_path_text(path: Path) -> str:
    return str(path.resolve(strict=False)).replace("\\", "/").casefold()


def _summary_level(
    audit_status: str,
    lifecycle_status: str,
    alerts: dict[str, Any],
) -> str:
    if audit_status in {AUDIT_ERROR, AUDIT_SAFETY_BLOCKED, AUDIT_MISMATCH}:
        return "CRITICAL"
    if audit_status in {AUDIT_INCOMPLETE, AUDIT_PASS_WITH_WARNINGS}:
        return "WARNING"
    if lifecycle_status != LIFECYCLE_DRAFT_READY:
        return "WARNING"
    if _strings(alerts.get("warnings")):
        return "WARNING"
    return "NORMAL"


def _headline(audit_status: str, lifecycle_status: str) -> str:
    if audit_status == AUDIT_PASS and lifecycle_status == LIFECYCLE_DRAFT_READY:
        return (
            "Notification draft, delivery preflight, and dispatch record are consistent. "
            "No external delivery side effects were detected."
        )
    if audit_status == AUDIT_SAFETY_BLOCKED:
        return "Notification delivery audit detected a safety anomaly. No notification was sent."
    if audit_status == AUDIT_MISMATCH:
        return "Notification delivery audit detected an artifact chain mismatch."
    if audit_status == AUDIT_INCOMPLETE:
        return "Notification delivery audit is incomplete because required artifacts are missing."
    if audit_status == AUDIT_ERROR:
        return "Notification delivery audit could not parse one or more artifacts."
    return "Notification delivery audit completed with warnings."


def _all_found_dates_match(
    as_of: date,
    draft_payload: dict[str, Any],
    preflight_payload: dict[str, Any],
    dispatch_payload: dict[str, Any],
) -> bool:
    expected = as_of.isoformat()
    for payload in (draft_payload, preflight_payload, dispatch_payload):
        if payload and payload.get("date") not in {None, expected}:
            return False
    return True


def _compute_draft_dispatch_hash(payload: dict[str, Any]) -> str:
    canonical_payload = _canonical_payload(payload, volatile_keys=DRAFT_HASH_VOLATILE_KEYS)
    canonical_json = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


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


def _contains_true_key(payload: dict[str, Any], keys: set[str]) -> bool:
    return any(value is True and key in keys for _, key, value in _walk_mapping(payload))


def _is_execution_field(key: str) -> bool:
    return (
        key in EXECUTION_FIELD_NAMES
        or key.endswith("_execution")
        or key.endswith("_executed")
        or "_executed_by_" in key
        or any(key.startswith(prefix) for prefix in EXECUTION_FIELD_PREFIXES)
    )


def _walk_mapping(value: object, prefix: str = "") -> list[tuple[str, str, object]]:
    records: list[tuple[str, str, object]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_prefix = f"{prefix}.{key_text}" if prefix else key_text
            records.append((child_prefix, key_text, child))
            records.extend(_walk_mapping(child, child_prefix))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            records.extend(_walk_mapping(child, f"{prefix}[{index}]"))
    return records


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: object) -> list[str]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    if isinstance(value, tuple):
        return [item for item in value if isinstance(item, str)]
    return []


def _string_value(value: object) -> str:
    return value if isinstance(value, str) else ""


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _bool_text(value: object) -> str:
    return "true" if value is True else "false"


def _artifact_row(label: str, artifact: object) -> str:
    record = _mapping(artifact)
    return (
        f"| {label} | `{record.get('status', STATUS_MISSING)}` | "
        f"`{record.get('path', '')}` | `{record.get('sha256', '')}` |"
    )


def _markdown_list(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _assert_audit_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-035 production_effect must be none")
    for field in OUTPUT_SAFETY_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-035 {field} must be true")
    for field in OUTPUT_SAFETY_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-035 {field} must be false")
