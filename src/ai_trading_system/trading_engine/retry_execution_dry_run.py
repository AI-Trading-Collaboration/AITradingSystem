from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "retry_execution_dry_run"
TASK_ID = "TRADING-038"
TASK_NAME = "Manual Approval Record / Retry Execution Dry Run"
SOURCE_TASK_ID = "TRADING-037"
MODE = "dry_run_only"
PRODUCTION_EFFECT_NONE = "none"

DRY_RUN_SOURCE_QUEUE_UNAVAILABLE = "SOURCE_QUEUE_UNAVAILABLE"
DRY_RUN_NOTHING_TO_DRY_RUN = "NOTHING_TO_DRY_RUN"
DRY_RUN_WAITING_FOR_MANUAL_APPROVAL = "WAITING_FOR_MANUAL_APPROVAL"
DRY_RUN_READY = "READY_FOR_DRY_RUN"
DRY_RUN_APPROVAL_MISMATCH = "APPROVAL_MISMATCH"
DRY_RUN_SAFETY_BLOCKED = "SAFETY_BLOCKED"

QUEUE_EMPTY = "EMPTY"
QUEUE_PENDING_APPROVAL = "PENDING_APPROVAL"
QUEUE_BLOCKED = "BLOCKED"
QUEUE_SAFETY_BLOCKED = "SAFETY_BLOCKED"
QUEUE_SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"

APPROVAL_APPROVED_FOR_DRY_RUN = "APPROVED_FOR_DRY_RUN"
APPROVAL_REJECTED = "REJECTED"
RETRY_PENDING_APPROVAL = "PENDING_APPROVAL"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "retry_execution_dry_run"


def default_retry_execution_dry_run_output_dir(project_root: Path = REPO_ROOT) -> Path:
    return project_root / "outputs" / "retry_execution_dry_run"


def default_retry_execution_dry_run_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"retry_execution_dry_run_{as_of.isoformat()}.json"


def default_retry_candidate_queue_output_dir(project_root: Path = REPO_ROOT) -> Path:
    return project_root / "outputs" / "retry_candidate_queue"


def default_manual_retry_approval_path(project_root: Path, as_of: date) -> Path:
    return (
        project_root
        / "inputs"
        / "manual_retry_approvals"
        / f"manual_retry_approval_{as_of.isoformat()}.json"
    )


def write_retry_execution_dry_run(
    *,
    as_of: date | None = None,
    queue_report_path: Path | None = None,
    approval_record_path: Path | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    project_root: Path = REPO_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    project_root = _normalize_path(project_root)
    output_dir = _resolve_project_path(project_root, output_dir)
    source_path = _resolve_queue_path(
        project_root=project_root,
        as_of=as_of,
        queue_report_path=queue_report_path,
        generated_at=generated,
    )
    queue_payload, source_available, source_parse_status, source_error = _read_queue_json(
        source_path
    )
    report_date = _report_date(
        as_of=as_of,
        source_path=source_path,
        source_payload=queue_payload,
        generated_at=generated,
    )
    approval_path = _resolve_approval_path(
        project_root=project_root,
        approval_record_path=approval_record_path,
        report_date=report_date,
    )
    (
        approval_payload,
        approval_available,
        approval_parse_status,
        approval_error,
    ) = _read_approval_json(approval_path)

    json_path = default_retry_execution_dry_run_json_path(output_dir, report_date)
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")
    payload = build_retry_execution_dry_run(
        as_of=report_date,
        queue_report_path=source_path,
        queue_payload=queue_payload,
        queue_available=source_available,
        queue_parse_status=source_parse_status,
        queue_error=source_error,
        approval_record_path=approval_path,
        approval_payload=approval_payload,
        approval_available=approval_available,
        approval_parse_status=approval_parse_status,
        approval_error=approval_error,
        output_json_path=json_path,
        output_markdown_path=markdown_path,
        run_log_path=log_path,
        generated_at=generated,
    )
    write_json_atomic(json_path, payload, sort_keys=False)
    write_text_atomic(markdown_path, render_retry_execution_dry_run_markdown(payload))
    write_text_atomic(log_path, render_retry_execution_dry_run_log(payload))
    return payload


def build_retry_execution_dry_run(
    *,
    as_of: date,
    queue_report_path: Path,
    queue_payload: dict[str, Any],
    queue_available: bool,
    queue_parse_status: str,
    queue_error: str,
    approval_record_path: Path,
    approval_payload: dict[str, Any],
    approval_available: bool,
    approval_parse_status: str,
    approval_error: str,
    output_json_path: Path | None = None,
    output_markdown_path: Path | None = None,
    run_log_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    source_queue = _source_queue(
        queue_report_path=queue_report_path,
        queue_payload=queue_payload,
        queue_available=queue_available,
        queue_parse_status=queue_parse_status,
        queue_error=queue_error,
    )
    candidates = _items(queue_payload.get("candidate_queue")) if queue_parse_status == "OK" else []
    queue_blocked_items = (
        _items(queue_payload.get("blocked_items")) if queue_parse_status == "OK" else []
    )
    approval_index = _approval_index(approval_payload, approval_parse_status)
    candidate_ids = {
        _candidate_id(candidate) for candidate in candidates if _candidate_id(candidate)
    }
    approved_ids = set(approval_index["approved"])
    rejected_ids = set(approval_index["rejected"])
    mismatched_ids = sorted((approved_ids | rejected_ids) - candidate_ids)
    valid_approved_ids = _valid_approved_candidate_ids(
        candidates=candidates,
        approved_ids=approved_ids,
    )
    safety_constraints_pass = _approval_safety_constraints_pass(
        approval_payload,
        approval_parse_status,
    )
    dry_run_status = _dry_run_status(
        source_parse_status=queue_parse_status,
        queue_status=_string_value(source_queue.get("queue_status")),
        candidates=candidates,
        approval_available=approval_available,
        approval_parse_status=approval_parse_status,
        valid_approved_ids=valid_approved_ids,
        mismatched_ids=mismatched_ids,
        safety_constraints_pass=safety_constraints_pass,
    )
    simulated_actions = _simulated_retry_actions(
        as_of=as_of,
        candidates=candidates,
        valid_approved_ids=valid_approved_ids,
        dry_run_status=dry_run_status,
    )
    blocked_items = _blocked_items(
        as_of=as_of,
        dry_run_status=dry_run_status,
        queue_status=_string_value(source_queue.get("queue_status")),
        candidates=candidates,
        queue_blocked_items=queue_blocked_items,
        approved_ids=approved_ids,
        rejected_ids=rejected_ids,
        mismatched_ids=mismatched_ids,
        approval_index=approval_index,
        approval_available=approval_available,
        approval_parse_status=approval_parse_status,
        approval_error=approval_error,
        safety_constraints_pass=safety_constraints_pass,
    )
    approval_record = _approval_record_summary(
        approval_record_path=approval_record_path,
        approval_available=approval_available,
        approval_parse_status=approval_parse_status,
        approval_error=approval_error,
        approval_payload=approval_payload,
        approval_index=approval_index,
        candidates=candidates,
        mismatched_ids=mismatched_ids,
    )
    summary = {
        "dry_run_status": dry_run_status,
        "total_candidates": len(candidates),
        "approved_for_dry_run": len(simulated_actions),
        "blocked_from_dry_run": len(blocked_items),
        "simulated_retry_actions": len(simulated_actions),
        "real_retry_allowed": False,
        "external_delivery_allowed": False,
        "production_state_mutation_allowed": False,
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "retry_execution_dry_run_only": True,
        "dry_run_only": True,
        "read_only": True,
        "approval_record_modified": False,
        "approval_state_modified": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "retry_executed": False,
        "actual_retry_executed": False,
        "external_delivery_executed": False,
        "delivery_state_mutated": False,
        "state_mutation_executed": False,
        "production_parameters_modified": False,
        "retry_candidate_queue_executed_by_dry_run": False,
        "notification_delivery_failure_classification_executed_by_dry_run": False,
        "notification_delivery_audit_executed_by_dry_run": False,
        "notification_draft_executed_by_dry_run": False,
        "delivery_preflight_executed_by_dry_run": False,
        "draft_dispatch_executed_by_dry_run": False,
        "operator_brief_executed_by_dry_run": False,
        "pipelines_executed_by_dry_run": False,
        "data_downloaded_by_dry_run": False,
        "apply_executed_by_dry_run": False,
        "rollback_executed_by_dry_run": False,
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
        "source_queue": source_queue,
        "approval_record": approval_record,
        "dry_run_summary": summary,
        "simulated_retry_actions": simulated_actions,
        "blocked_items": blocked_items,
        "recommended_actions": _recommended_actions(
            dry_run_status=dry_run_status,
            queue_error=queue_error,
            approval_error=approval_error,
            blocked_items=blocked_items,
        ),
        "safety_invariants": {
            "dry_run_only": True,
            "no_external_delivery": True,
            "no_retry_execution": True,
            "no_state_mutation": True,
            "no_production_parameter_change": True,
            "approval_record_is_input_only": True,
            "dashboard_read_only": True,
        },
        "output_artifacts": {
            "retry_execution_dry_run_json": {
                "path": "" if output_json_path is None else str(output_json_path)
            },
            "retry_execution_dry_run_markdown": {
                "path": "" if output_markdown_path is None else str(output_markdown_path)
            },
            "run_log": {"path": "" if run_log_path is None else str(run_log_path)},
        },
    }
    _assert_safety_invariants(payload)
    return payload


def find_latest_retry_candidate_queue(
    *,
    project_root: Path = REPO_ROOT,
    as_of: date | None = None,
) -> Path | None:
    queue_root = default_retry_candidate_queue_output_dir(_normalize_path(project_root))
    if not queue_root.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in queue_root.glob("retry_candidate_queue_*.json"):
        parsed = _queue_date_from_path(path)
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
    fail_on_approval_mismatch: bool = False,
) -> bool:
    summary = _mapping(payload.get("dry_run_summary"))
    status = summary.get("dry_run_status")
    if fail_on_safety_blocked and status == DRY_RUN_SAFETY_BLOCKED:
        return True
    return bool(fail_on_approval_mismatch and status == DRY_RUN_APPROVAL_MISMATCH)


def render_retry_execution_dry_run_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    source = _mapping(payload.get("source_queue"))
    approval = _mapping(payload.get("approval_record"))
    summary = _mapping(payload.get("dry_run_summary"))
    actions = _items(payload.get("simulated_retry_actions"))
    blocked_items = _items(payload.get("blocked_items"))
    safety = _mapping(payload.get("safety_invariants"))
    dry_run_status = _string_value(summary.get("dry_run_status")) or (
        DRY_RUN_SOURCE_QUEUE_UNAVAILABLE
    )

    lines = [
        "# Manual Approval Record / Retry Execution Dry Run",
        "",
        "## Status Banner",
        "",
        _status_banner(dry_run_status),
        "",
    ]
    if dry_run_status == DRY_RUN_SAFETY_BLOCKED:
        lines.extend(
            [
                "CRITICAL: Safety blocked. Retry dry-run is not allowed.",
                "",
            ]
        )
    lines.extend(
        [
            "## Source Queue",
            "",
            f"- Source task: `{source.get('task_id', SOURCE_TASK_ID)}`",
            f"- Queue report: `{source.get('queue_report_path', '')}`",
            f"- Queue status: `{source.get('queue_status', 'UNKNOWN')}`",
            f"- Source parse status: `{source.get('source_parse_status', 'UNKNOWN')}`",
            "",
            "## Approval Record",
            "",
            f"- Approval record path: `{approval.get('approval_record_path', '')}`",
            f"- Approval available: `{_bool_text(approval.get('approval_record_available'))}`",
            f"- Approval parse status: `{approval.get('approval_parse_status', 'UNKNOWN')}`",
            f"- Approved candidate count: `{approval.get('approved_candidate_count', 0)}`",
            f"- Rejected candidate count: `{approval.get('rejected_candidate_count', 0)}`",
            f"- Unapproved candidate count: `{approval.get('unapproved_candidate_count', 0)}`",
            "",
            "## Dry Run Summary",
            "",
            f"- Dry run status: `{summary.get('dry_run_status', '')}`",
            f"- Total candidates: `{summary.get('total_candidates', 0)}`",
            f"- Approved for dry run: `{summary.get('approved_for_dry_run', 0)}`",
            f"- Blocked from dry run: `{summary.get('blocked_from_dry_run', 0)}`",
            f"- Simulated retry actions: `{summary.get('simulated_retry_actions', 0)}`",
            f"- Real retry allowed: `{_bool_text(summary.get('real_retry_allowed'))}`",
            f"- External delivery allowed: "
            f"`{_bool_text(summary.get('external_delivery_allowed'))}`",
            f"- Production mutation allowed: "
            f"`{_bool_text(summary.get('production_state_mutation_allowed'))}`",
            "",
            "## Simulated Retry Actions",
            "",
        ]
    )
    if not actions:
        lines.extend(["No simulated retry actions.", ""])
    else:
        for index, action in enumerate(actions, start=1):
            lines.extend(
                [
                    f"### Action {index:03d}",
                    "",
                    f"- Candidate ID: `{action.get('candidate_id', '')}`",
                    f"- Dry-run action ID: `{action.get('dry_run_action_id', '')}`",
                    f"- Source category: `{action.get('source_category', '')}`",
                    f"- Would retry: `{_bool_text(action.get('would_retry'))}`",
                    "- Actual retry executed: "
                    f"`{_bool_text(action.get('actual_retry_executed'))}`",
                    "- External delivery executed: "
                    f"`{_bool_text(action.get('external_delivery_executed'))}`",
                    "- State mutation executed: "
                    f"`{_bool_text(action.get('state_mutation_executed'))}`",
                    f"- Safety result: `{action.get('safety_result', '')}`",
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
                    f"- Candidate ID: `{item.get('candidate_id', '')}`",
                    f"- Block reason: `{item.get('block_reason', '')}`",
                    f"- Safety result: `{item.get('safety_result', '')}`",
                    "",
                ]
            )
    lines.extend(
        [
            "## Recommended Actions",
            "",
            *_markdown_list(_strings(payload.get("recommended_actions"))),
            "",
            "## Safety Invariants",
            "",
            f"- Dry run only: `{_bool_text(safety.get('dry_run_only'))}`",
            f"- No external delivery: `{_bool_text(safety.get('no_external_delivery'))}`",
            f"- No retry execution: `{_bool_text(safety.get('no_retry_execution'))}`",
            f"- No state mutation: `{_bool_text(safety.get('no_state_mutation'))}`",
            "- No production parameter change: "
            f"`{_bool_text(safety.get('no_production_parameter_change'))}`",
            "- Approval record is input only: "
            f"`{_bool_text(safety.get('approval_record_is_input_only'))}`",
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


def render_retry_execution_dry_run_log(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    source = _mapping(payload.get("source_queue"))
    approval = _mapping(payload.get("approval_record"))
    summary = _mapping(payload.get("dry_run_summary"))
    outputs = _mapping(payload.get("output_artifacts"))
    return "\n".join(
        [
            f"task_id={metadata.get('task_id', TASK_ID)}",
            f"generated_at={metadata.get('generated_at', '')}",
            f"dry_run_status={summary.get('dry_run_status', '')}",
            f"source_queue_status={source.get('queue_status', 'UNKNOWN')}",
            f"source_parse_status={source.get('source_parse_status', 'UNKNOWN')}",
            f"approval_record_available={_bool_text(approval.get('approval_record_available'))}",
            f"approval_parse_status={approval.get('approval_parse_status', 'UNKNOWN')}",
            f"total_candidates={summary.get('total_candidates', 0)}",
            f"approved_for_dry_run={summary.get('approved_for_dry_run', 0)}",
            f"blocked_from_dry_run={summary.get('blocked_from_dry_run', 0)}",
            f"simulated_retry_actions={summary.get('simulated_retry_actions', 0)}",
            "real_retry_allowed=false",
            "external_delivery_allowed=false",
            "production_state_mutation_allowed=false",
            "production_effect=none",
            "manual_review_only=true",
            "dry_run_only=true",
            "no_external_delivery=true",
            "no_retry_execution=true",
            "no_state_mutation=true",
            "no_production_parameter_change=true",
            "approval_record_is_input_only=true",
            "dashboard_read_only=true",
            "retry_execution_dry_run_json="
            f"{_mapping(outputs.get('retry_execution_dry_run_json')).get('path', '')}",
            "retry_execution_dry_run_markdown="
            f"{_mapping(outputs.get('retry_execution_dry_run_markdown')).get('path', '')}",
            f"run_log={_mapping(outputs.get('run_log')).get('path', '')}",
            "",
        ]
    )


def _resolve_queue_path(
    *,
    project_root: Path,
    as_of: date | None,
    queue_report_path: Path | None,
    generated_at: datetime,
) -> Path:
    if queue_report_path is not None:
        return _resolve_project_path(project_root, queue_report_path)
    latest = find_latest_retry_candidate_queue(project_root=project_root, as_of=as_of)
    if latest is not None:
        return latest
    fallback_date = as_of or generated_at.date()
    return (
        project_root
        / "outputs"
        / "retry_candidate_queue"
        / f"retry_candidate_queue_{fallback_date.isoformat()}.json"
    )


def _resolve_approval_path(
    *,
    project_root: Path,
    approval_record_path: Path | None,
    report_date: date,
) -> Path:
    if approval_record_path is not None:
        return _resolve_project_path(project_root, approval_record_path)
    input_path = default_manual_retry_approval_path(project_root, report_date)
    if input_path.exists():
        return input_path
    config_candidates = (
        project_root / "configs" / f"manual_retry_approval_{report_date.isoformat()}.json",
        project_root / "config" / f"manual_retry_approval_{report_date.isoformat()}.json",
    )
    for path in config_candidates:
        if path.exists():
            return path
    return input_path


def _read_queue_json(path: Path) -> tuple[dict[str, Any], bool, str, str]:
    if not path.exists():
        return {}, False, "MISSING", f"Source queue report not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, True, "MALFORMED_JSON", f"Source queue JSON malformed: {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, True, "INVALID_SHAPE", f"Source queue JSON must be an object: {path}"
    if payload.get("report_type") != "retry_candidate_queue":
        return (
            {},
            True,
            "INVALID_REPORT_TYPE",
            f"Source queue report_type is not TRADING-037: {path}",
        )
    return payload, True, "OK", ""


def _read_approval_json(path: Path) -> tuple[dict[str, Any], bool, str, str]:
    if not path.exists():
        return {}, False, "MISSING", f"Approval record not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {}, True, "MALFORMED_JSON", f"Approval record JSON malformed: {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, True, "INVALID_SHAPE", f"Approval record JSON must be an object: {path}"
    return payload, True, "OK", ""


def _report_date(
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
    path_date = _queue_date_from_path(source_path)
    if path_date is not None:
        return path_date
    return generated_at.date()


def _source_queue(
    *,
    queue_report_path: Path,
    queue_payload: dict[str, Any],
    queue_available: bool,
    queue_parse_status: str,
    queue_error: str,
) -> dict[str, Any]:
    summary = _mapping(queue_payload.get("queue_summary"))
    return {
        "task_id": SOURCE_TASK_ID,
        "queue_report_path": str(queue_report_path),
        "queue_status": _string_value(summary.get("queue_status"))
        or (QUEUE_SOURCE_UNAVAILABLE if queue_parse_status != "OK" else "UNKNOWN"),
        "source_available": queue_available,
        "source_parse_status": queue_parse_status,
        "source_error": queue_error,
    }


def _approval_index(
    approval_payload: dict[str, Any],
    approval_parse_status: str,
) -> dict[str, dict[str, Any]]:
    approved: dict[str, dict[str, Any]] = {}
    rejected: dict[str, dict[str, Any]] = {}
    if approval_parse_status != "OK":
        return {"approved": approved, "rejected": rejected}
    for item in _items(approval_payload.get("approved_candidates")):
        candidate_id = _string_value(item.get("candidate_id"))
        if candidate_id and item.get("approval_status") == APPROVAL_APPROVED_FOR_DRY_RUN:
            approved[candidate_id] = item
    for item in _items(approval_payload.get("rejected_candidates")):
        candidate_id = _string_value(item.get("candidate_id"))
        if candidate_id and item.get("approval_status") == APPROVAL_REJECTED:
            rejected[candidate_id] = item
    return {"approved": approved, "rejected": rejected}


def _approval_record_summary(
    *,
    approval_record_path: Path,
    approval_available: bool,
    approval_parse_status: str,
    approval_error: str,
    approval_payload: dict[str, Any],
    approval_index: dict[str, dict[str, Any]],
    candidates: list[dict[str, Any]],
    mismatched_ids: list[str],
) -> dict[str, Any]:
    approved = _mapping(approval_index.get("approved"))
    rejected = _mapping(approval_index.get("rejected"))
    approved_or_rejected = set(approved) | set(rejected)
    candidate_ids = {
        _candidate_id(candidate) for candidate in candidates if _candidate_id(candidate)
    }
    unapproved_count = len(candidate_ids - approved_or_rejected)
    return {
        "approval_record_path": str(approval_record_path),
        "approval_record_available": approval_available,
        "approval_parse_status": approval_parse_status,
        "approval_error": approval_error,
        "approved_candidate_count": len(approved),
        "rejected_candidate_count": len(rejected),
        "unapproved_candidate_count": unapproved_count,
        "approval_mismatch_count": len(mismatched_ids),
        "safety_constraints": _approval_safety_constraints_display(
            approval_available=approval_available,
            approval_parse_status=approval_parse_status,
            approval_payload=approval_payload,
        ),
    }


def _approval_safety_constraints_display(
    *,
    approval_available: bool,
    approval_parse_status: str,
    approval_payload: dict[str, Any],
) -> dict[str, Any]:
    constraints = _mapping(approval_payload.get("safety_constraints"))
    approval_constraints_available = approval_available and approval_parse_status == "OK"
    return {
        "dry_run_only": approval_constraints_available and constraints.get("dry_run_only") is True,
        "real_retry_allowed": constraints.get("real_retry_allowed") is True,
        "external_delivery_allowed": constraints.get("external_delivery_allowed") is True,
        "production_state_mutation_allowed": constraints.get("production_state_mutation_allowed")
        is True,
    }


def _dry_run_status(
    *,
    source_parse_status: str,
    queue_status: str,
    candidates: list[dict[str, Any]],
    approval_available: bool,
    approval_parse_status: str,
    valid_approved_ids: list[str],
    mismatched_ids: list[str],
    safety_constraints_pass: bool,
) -> str:
    if source_parse_status != "OK":
        return DRY_RUN_SOURCE_QUEUE_UNAVAILABLE
    if queue_status == QUEUE_SAFETY_BLOCKED:
        return DRY_RUN_SAFETY_BLOCKED
    if queue_status == QUEUE_EMPTY or not candidates:
        return DRY_RUN_NOTHING_TO_DRY_RUN
    if not approval_available or approval_parse_status != "OK":
        return DRY_RUN_WAITING_FOR_MANUAL_APPROVAL
    if mismatched_ids:
        return DRY_RUN_APPROVAL_MISMATCH
    if valid_approved_ids and not safety_constraints_pass:
        return DRY_RUN_SAFETY_BLOCKED
    if valid_approved_ids:
        return DRY_RUN_READY
    return DRY_RUN_WAITING_FOR_MANUAL_APPROVAL


def _valid_approved_candidate_ids(
    *,
    candidates: list[dict[str, Any]],
    approved_ids: set[str],
) -> list[str]:
    valid_ids: list[str] = []
    for candidate in candidates:
        candidate_id = _candidate_id(candidate)
        if candidate_id in approved_ids and candidate.get("retry_status") == RETRY_PENDING_APPROVAL:
            valid_ids.append(candidate_id)
    return valid_ids


def _approval_safety_constraints_pass(
    approval_payload: dict[str, Any],
    approval_parse_status: str,
) -> bool:
    if approval_parse_status != "OK":
        return False
    constraints = _mapping(approval_payload.get("safety_constraints"))
    return (
        constraints.get("dry_run_only") is True
        and constraints.get("real_retry_allowed") is False
        and constraints.get("external_delivery_allowed") is False
        and constraints.get("production_state_mutation_allowed") is False
    )


def _simulated_retry_actions(
    *,
    as_of: date,
    candidates: list[dict[str, Any]],
    valid_approved_ids: list[str],
    dry_run_status: str,
) -> list[dict[str, Any]]:
    if dry_run_status != DRY_RUN_READY:
        return []
    approved_set = set(valid_approved_ids)
    actions: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = _candidate_id(candidate)
        if candidate_id not in approved_set:
            continue
        sequence = len(actions) + 1
        actions.append(
            {
                "candidate_id": candidate_id,
                "dry_run_action_id": f"dry_run_retry_{as_of.isoformat()}_{sequence:03d}",
                "source_category": _string_value(candidate.get("source_category")),
                "simulated_channel": "notification_channel_placeholder",
                "simulated_target": "redacted_or_placeholder",
                "payload_available": False,
                "would_retry": True,
                "actual_retry_executed": False,
                "external_delivery_executed": False,
                "state_mutation_executed": False,
                "safety_result": "PASS",
            }
        )
    return actions


def _blocked_items(
    *,
    as_of: date,
    dry_run_status: str,
    queue_status: str,
    candidates: list[dict[str, Any]],
    queue_blocked_items: list[dict[str, Any]],
    approved_ids: set[str],
    rejected_ids: set[str],
    mismatched_ids: list[str],
    approval_index: dict[str, dict[str, Any]],
    approval_available: bool,
    approval_parse_status: str,
    approval_error: str,
    safety_constraints_pass: bool,
) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []

    def add(candidate_id: str, reason: str, status: str = "BLOCKED") -> None:
        blocked.append(
            {
                "blocked_item_id": f"dry_run_blocked_{as_of.isoformat()}_{len(blocked) + 1:03d}",
                "candidate_id": candidate_id,
                "approval_status": status,
                "block_reason": reason,
                "safety_result": "FAIL" if status == "SAFETY_BLOCKED" else "BLOCKED",
            }
        )

    if dry_run_status == DRY_RUN_SOURCE_QUEUE_UNAVAILABLE:
        add("", "TRADING-037 retry candidate queue is missing or malformed.")
        return blocked
    if dry_run_status == DRY_RUN_SAFETY_BLOCKED and queue_status == QUEUE_SAFETY_BLOCKED:
        add(
            "",
            "TRADING-037 queue_status is SAFETY_BLOCKED. Retry dry-run is not allowed.",
            "SAFETY_BLOCKED",
        )
        return blocked
    if dry_run_status == DRY_RUN_SAFETY_BLOCKED and not safety_constraints_pass:
        add(
            "",
            "Approval safety constraints do not preserve dry-run-only execution.",
            "SAFETY_BLOCKED",
        )
    if dry_run_status == DRY_RUN_NOTHING_TO_DRY_RUN:
        for item in queue_blocked_items:
            add(
                _string_value(item.get("blocked_item_id")),
                _string_value(item.get("block_reason")) or "TRADING-037 item is blocked.",
            )
        return blocked
    if approval_available and approval_parse_status != "OK":
        add("", approval_error or "Approval record is malformed or invalid.")
        return blocked
    for candidate_id in mismatched_ids:
        add(candidate_id, "Approval record references candidate_id not found in TRADING-037 queue.")
    rejected = _mapping(approval_index.get("rejected"))
    for candidate in candidates:
        candidate_id = _candidate_id(candidate)
        if not candidate_id:
            continue
        if candidate_id in rejected_ids:
            rejection = _mapping(rejected.get(candidate_id))
            add(
                candidate_id,
                _string_value(rejection.get("rejection_reason"))
                or "Candidate was rejected by manual approval record.",
                APPROVAL_REJECTED,
            )
            continue
        if candidate_id not in approved_ids:
            reason = (
                "Manual approval record does not approve candidate for dry-run."
                if approval_available
                else "Waiting for manual approval record before retry dry-run."
            )
            add(candidate_id, reason, "UNAPPROVED")
            continue
        if candidate.get("retry_status") != RETRY_PENDING_APPROVAL:
            add(candidate_id, "Candidate retry_status is not PENDING_APPROVAL.")
    return blocked


def _recommended_actions(
    *,
    dry_run_status: str,
    queue_error: str,
    approval_error: str,
    blocked_items: list[dict[str, Any]],
) -> list[str]:
    if dry_run_status == DRY_RUN_SOURCE_QUEUE_UNAVAILABLE:
        return [
            queue_error or "Restore the TRADING-037 retry candidate queue before dry-run.",
            "Do not infer retry actions from a missing or malformed queue artifact.",
        ]
    if dry_run_status == DRY_RUN_NOTHING_TO_DRY_RUN:
        return ["No approved retry dry-run action is needed for the current queue."]
    if dry_run_status == DRY_RUN_SAFETY_BLOCKED:
        return [
            "CRITICAL: Safety blocked. Retry dry-run is not allowed.",
            "Resolve queue or approval safety blockers before retry dry-run review.",
        ]
    if dry_run_status == DRY_RUN_APPROVAL_MISMATCH:
        return [
            "Align approval candidate_id values with the TRADING-037 retry candidate queue.",
            "Do not proceed to any retry execution layer while approval mismatch exists.",
        ]
    if dry_run_status == DRY_RUN_WAITING_FOR_MANUAL_APPROVAL:
        action = approval_error or "Create or update a manual approval record for dry-run only."
        return [
            action,
            "Only APPROVED_FOR_DRY_RUN candidates may produce simulated retry actions.",
        ]
    if blocked_items:
        return [
            "Review blocked candidates before considering future retry execution.",
            "Dry-run actions remain simulation-only and do not authorize real delivery.",
        ]
    return [
        "Review the simulated retry actions and safety invariants before any future executor work.",
        "TRADING-038 does not authorize or execute real retry delivery.",
    ]


def _status_banner(dry_run_status: str) -> str:
    if dry_run_status == DRY_RUN_READY:
        return "READY_FOR_DRY_RUN: Approved candidates have simulated retry actions."
    if dry_run_status == DRY_RUN_WAITING_FOR_MANUAL_APPROVAL:
        return "WAITING_FOR_MANUAL_APPROVAL: No approved dry-run candidate is available."
    if dry_run_status == DRY_RUN_NOTHING_TO_DRY_RUN:
        return "NOTHING_TO_DRY_RUN: No retry candidate requires dry-run action."
    if dry_run_status == DRY_RUN_APPROVAL_MISMATCH:
        return "APPROVAL_MISMATCH: Approval record references unknown candidate ids."
    if dry_run_status == DRY_RUN_SAFETY_BLOCKED:
        return "SAFETY_BLOCKED: Retry dry-run is prohibited."
    return "SOURCE_QUEUE_UNAVAILABLE: Source retry candidate queue is missing or malformed."


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("TRADING-038 production_effect must be none")
    for field in (
        "manual_review_only",
        "retry_execution_dry_run_only",
        "dry_run_only",
        "read_only",
    ):
        if payload.get(field) is not True:
            raise ValueError(f"TRADING-038 {field} must be true")
    false_fields = (
        "approval_record_modified",
        "approval_state_modified",
        "email_sent",
        "gmail_draft_created",
        "gmail_draft_modified",
        "slack_sent",
        "discord_sent",
        "webhook_called",
        "mobile_push_sent",
        "retry_executed",
        "actual_retry_executed",
        "external_delivery_executed",
        "delivery_state_mutated",
        "state_mutation_executed",
        "production_parameters_modified",
        "retry_candidate_queue_executed_by_dry_run",
        "notification_delivery_failure_classification_executed_by_dry_run",
        "notification_delivery_audit_executed_by_dry_run",
        "notification_draft_executed_by_dry_run",
        "delivery_preflight_executed_by_dry_run",
        "draft_dispatch_executed_by_dry_run",
        "operator_brief_executed_by_dry_run",
        "pipelines_executed_by_dry_run",
        "data_downloaded_by_dry_run",
        "apply_executed_by_dry_run",
        "rollback_executed_by_dry_run",
        "broker_execution",
        "replay_execution",
        "trading_execution",
    )
    for field in false_fields:
        if payload.get(field) is not False:
            raise ValueError(f"TRADING-038 {field} must be false")
    summary = _mapping(payload.get("dry_run_summary"))
    for field in (
        "real_retry_allowed",
        "external_delivery_allowed",
        "production_state_mutation_allowed",
    ):
        if summary.get(field) is not False:
            raise ValueError(f"TRADING-038 dry_run_summary.{field} must be false")
    safety = _mapping(payload.get("safety_invariants"))
    for field in (
        "dry_run_only",
        "no_external_delivery",
        "no_retry_execution",
        "no_state_mutation",
        "no_production_parameter_change",
        "approval_record_is_input_only",
        "dashboard_read_only",
    ):
        if safety.get(field) is not True:
            raise ValueError(f"TRADING-038 safety_invariants.{field} must be true")
    for action in _items(payload.get("simulated_retry_actions")):
        if action.get("actual_retry_executed") is not False:
            raise ValueError("TRADING-038 simulated action actual_retry_executed must be false")
        if action.get("external_delivery_executed") is not False:
            raise ValueError(
                "TRADING-038 simulated action external_delivery_executed must be false"
            )
        if action.get("state_mutation_executed") is not False:
            raise ValueError("TRADING-038 simulated action state_mutation_executed must be false")


def _queue_date_from_path(path: Path) -> date | None:
    raw_date = path.stem.removeprefix("retry_candidate_queue_")
    return _parse_iso_date(raw_date)


def _candidate_id(candidate: dict[str, Any]) -> str:
    return _string_value(candidate.get("candidate_id"))


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


def _markdown_list(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _bool_text(value: object) -> str:
    return "true" if value is True else "false"


def _isoformat(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")
