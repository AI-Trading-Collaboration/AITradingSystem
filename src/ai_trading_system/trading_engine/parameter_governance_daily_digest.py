from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "parameter_governance_daily_digest"
RUN_REPORT_TYPE = "parameter_governance_daily_digest_run"
TASK_ID = "TRADING-021"
SUMMARY_TASK_ID = "TRADING-019"
MODE = "parameter_governance_daily_digest_only"
PRODUCTION_EFFECT_NONE = "none"
DEFAULT_LOOKBACK_DAYS = 7

DIGEST_OK = "OK"
DIGEST_WATCH = "WATCH"
DIGEST_ACTION_REQUIRED = "ACTION_REQUIRED"
DIGEST_URGENT = "URGENT"
DIGEST_INPUT_MISSING = "INPUT_MISSING"
DIGEST_INPUT_INVALID = "INPUT_INVALID"
DIGEST_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DIGEST_ERROR = "ERROR"

SUMMARY_NORMAL = "NORMAL"
SUMMARY_WATCH = "WATCH"
SUMMARY_ACTION = "ACTION"
SUMMARY_URGENT = "URGENT"
SUMMARY_UNKNOWN = "UNKNOWN"

STATE_SAFE_OBSERVATION = "SAFE_OBSERVATION"
STATE_SHADOW_LEARNING = "SHADOW_LEARNING"
STATE_APPLIED_NEEDS_MONITORING = "APPLIED_NEEDS_MONITORING"
STATE_ROLLBACK_COMPLETED = "ROLLBACK_COMPLETED"
STATE_SAFETY_ANOMALY = "SAFETY_ANOMALY"

ACTION_NONE = "NONE"
ACTION_WATCH = "WATCH"
ACTION_REVIEW_REQUIRED = "REVIEW_REQUIRED"
ACTION_APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
ACTION_ROLLBACK_REVIEW_REQUIRED = "ROLLBACK_REVIEW_REQUIRED"
ACTION_URGENT = "URGENT"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

PIPELINE_CONTRACT: dict[str, Any] = {
    "reads_existing_artifacts_only": True,
    "runs_shadow_iteration_pipeline": False,
    "runs_comparison_pipeline": False,
    "runs_multi_day_review_pipeline": False,
    "runs_promotion_proposal_pipeline": False,
    "runs_apply_preflight_pipeline": False,
    "runs_promotion_apply": False,
    "runs_promotion_rollback": False,
    "runs_lifecycle_audit_pipeline": False,
    "runs_governance_summary_pipeline": False,
    "runs_web_view_render_script": False,
    "runs_daily_digest_script": False,
    "runs_scoring_pipeline": False,
    "runs_broker_runner": False,
    "runs_paper_runner": False,
    "runs_replay_runner": False,
    "writes_production_profile": False,
    "writes_production_weights": False,
    "writes_shadow_weights": False,
    "writes_approved_profile": False,
    "promotes_shadow_to_production": False,
    "changes_daily_dashboard_main_conclusion": False,
    "triggers_trade": False,
    "production_effect": PRODUCTION_EFFECT_NONE,
    "manual_review_only": True,
    "digest_only": True,
    "governance_only": True,
}


def default_governance_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "governance"


def default_digest_root(data_root: Path) -> Path:
    return default_governance_root(data_root) / "digests"


def default_daily_digest_json_path(data_root: Path, as_of: date) -> Path:
    return default_digest_root(data_root) / (
        f"parameter_governance_daily_digest_{as_of.isoformat()}.json"
    )


def default_daily_digest_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_digest_root(data_root)
        / "logs"
        / f"parameter_governance_daily_digest_run_{as_of.isoformat()}.json"
    )


def default_governance_summary_json_path(data_root: Path, as_of: date) -> Path:
    return default_governance_root(data_root) / (
        f"parameter_governance_summary_{as_of.isoformat()}.json"
    )


def default_web_view_metadata_path(data_root: Path, as_of: date) -> Path:
    return (
        default_governance_root(data_root)
        / "web"
        / f"parameter_governance_web_view_{as_of.isoformat()}.json"
    )


def write_parameter_governance_daily_digest(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    governance_summary_file: Path | None = None,
    web_view_metadata_file: Path | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    fail_on_safety_anomaly: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_daily_digest_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_daily_digest_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_parameter_governance_daily_digest_payload(
            as_of=as_of,
            data_root=data_root,
            governance_summary_file=governance_summary_file,
            web_view_metadata_file=web_view_metadata_file,
            lookback_days=lookback_days,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
        )
    except Exception as exc:  # pragma: no cover - defensive report path
        payload = _error_payload(
            as_of=as_of,
            data_root=data_root,
            governance_summary_file=governance_summary_file,
            web_view_metadata_file=web_view_metadata_file,
            lookback_days=lookback_days,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    write_json_atomic(output_json_path, payload, sort_keys=False)
    write_text_atomic(output_md_path, render_parameter_governance_daily_digest_markdown(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    write_json_atomic(run_log_json_path, run_log, sort_keys=False)
    write_text_atomic(run_log_md_path, render_parameter_governance_daily_digest_run_log(run_log))

    has_safety_anomaly = _mapping(payload.get("daily_readout")).get("has_safety_anomaly") is True
    if fail_on_safety_anomaly and has_safety_anomaly:
        raise SystemExit(2)
    return payload


def build_parameter_governance_daily_digest_payload(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    governance_summary_file: Path | None = None,
    web_view_metadata_file: Path | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_daily_digest_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_daily_digest_run_log_json_path(
        data_root, as_of
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")

    input_paths = _resolve_input_paths(
        as_of=as_of,
        data_root=data_root,
        governance_summary_file=governance_summary_file,
        web_view_metadata_file=web_view_metadata_file,
        lookback_days=lookback_days,
    )
    input_artifacts = {
        key: _artifact_record(path) for key, path in input_paths.items() if path is not None
    }
    summary_path = input_paths["governance_summary"]
    summary_payload, input_status, input_reason = _load_governance_summary(summary_path)
    safety_validation = _summary_safety_validation(
        summary_payload,
        input_status=input_status,
        input_reason=input_reason,
    )
    web_metadata = _read_json_object(input_paths.get("web_view_metadata"))

    digest_status = _digest_status(
        summary=summary_payload,
        input_status=input_status,
        safety_validation=safety_validation,
    )
    summary_level = _summary_level(digest_status)
    alerts = _alerts(
        summary=summary_payload,
        digest_status=digest_status,
        input_reason=input_reason,
        safety_validation=safety_validation,
    )
    weight_snapshot = _weight_snapshot(summary_payload, alerts=alerts)
    pending_items = _pending_items(summary_payload)
    governance_snapshot = _governance_snapshot(summary_payload)
    daily_readout = _daily_readout(
        summary=summary_payload,
        digest_status=digest_status,
        pending_items=pending_items,
        alerts=alerts,
    )
    headline = _headline(digest_status)
    links = _links(
        summary=summary_payload,
        web_metadata=web_metadata,
        data_root=data_root,
        as_of=as_of,
        output_md_path=output_md_path,
    )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": _isoformat_z(generated),
        "lookback_days": lookback_days,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "digest_only": True,
        "governance_only": True,
        "apply_executed_by_digest": False,
        "rollback_executed_by_digest": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "digest_status": digest_status,
        "headline": headline,
        "summary_level": summary_level,
        "input_artifacts": input_artifacts,
        "governance_snapshot": governance_snapshot,
        "daily_readout": daily_readout,
        "weight_snapshot": weight_snapshot,
        "pending_items": pending_items,
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(digest_status),
        "links": links,
        "safety_validation": safety_validation,
        "output_artifacts": {
            "json": {"path": str(output_json_path)},
            "markdown": {"path": str(output_md_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
        "pipeline_contract": dict(PIPELINE_CONTRACT),
        "audit": {
            "created_by": "scripts/run_parameter_governance_daily_digest.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_digest_artifacts": True,
        },
    }
    _assert_digest_safety_invariants(payload)
    return payload


def render_parameter_governance_daily_digest_markdown(payload: dict[str, Any]) -> str:
    snapshot = _mapping(payload.get("governance_snapshot"))
    readout = _mapping(payload.get("daily_readout"))
    pending = _mapping(payload.get("pending_items"))
    weights = _mapping(payload.get("weight_snapshot"))
    alerts = _mapping(payload.get("alerts"))
    links = _mapping(payload.get("links"))
    digest_status = _string_value(payload.get("digest_status")) or DIGEST_ERROR

    lines = [f"# Parameter Governance Daily Digest - {payload.get('date')}", ""]
    if digest_status == DIGEST_URGENT:
        lines.extend(["## URGENT: Manual Attention Required", ""])
    elif digest_status == DIGEST_ACTION_REQUIRED:
        lines.extend(["## Action Required", ""])
    elif digest_status == DIGEST_SAFETY_BLOCKED:
        lines.extend(["## Digest Safety Blocked", ""])
        lines.extend(
            [
                (
                    "Digest generation was safety-blocked because governance summary safety "
                    "fields are invalid."
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## 1. Today's Status",
            "",
            f"- Digest Status: `{digest_status}`",
            f"- Governance State: `{snapshot.get('governance_state', 'MISSING')}`",
            f"- Action Required: `{str(snapshot.get('action_required') is True).lower()}`",
            f"- Action Level: `{snapshot.get('action_level', 'NONE')}`",
            f"- Recommended Action: {snapshot.get('recommended_action', '')}",
            "",
            "## 2. One-line Summary",
            "",
            str(payload.get("headline") or ""),
            "",
            "## 3. Safety Check",
            "",
            f"- Safety Boundary Status: `{snapshot.get('safety_boundary_status', 'MISSING')}`",
            f"- Broker Execution: `{str(payload.get('broker_execution') is True).lower()}`",
            f"- Replay Execution: `{str(payload.get('replay_execution') is True).lower()}`",
            f"- Trading Execution: `{str(payload.get('trading_execution') is True).lower()}`",
            "",
            "## 4. Pending Actions",
            "",
            "| Item | Pending |",
            "|---|---:|",
            f"| Proposal Review | `{_bool_text(pending.get('pending_proposal_review'))}` |",
            f"| Preflight | `{_bool_text(pending.get('pending_preflight'))}` |",
            f"| Apply | `{_bool_text(pending.get('pending_apply'))}` |",
            f"| Rollback | `{_bool_text(pending.get('pending_rollback'))}` |",
            f"| Lifecycle Audit | `{_bool_text(pending.get('pending_lifecycle_audit'))}` |",
            "",
            "## 5. Production vs Shadow Weights",
            "",
            "| Weight Key | Production | Shadow | Delta |",
            "|---|---:|---:|---:|",
        ]
    )
    delta_summary = _records(weights.get("delta_summary"))
    if delta_summary:
        for row in delta_summary:
            lines.append(
                "| "
                f"{row.get('weight_key')} | {_format_float(row.get('production'))} | "
                f"{_format_float(row.get('shadow'))} | "
                f"{_format_signed_float(row.get('delta'))} |"
            )
    else:
        lines.append("| NOT_AVAILABLE | NA | NA | NA |")

    lines.extend(
        [
            "",
            "## 6. Latest Review / Lifecycle",
            "",
            f"- Shadow Review Decision: `{readout.get('shadow_review_decision', 'MISSING')}`",
            f"- Latest Lifecycle State: `{readout.get('latest_lifecycle_state', 'MISSING')}`",
            f"- Latest Apply Status: `{readout.get('latest_apply_status', 'MISSING')}`",
            f"- Latest Rollback Status: `{readout.get('latest_rollback_status', 'MISSING')}`",
            "",
            "## 7. Alerts",
            "",
            "### Critical",
            "",
        ]
    )
    lines.extend(_markdown_bullets(_strings(alerts.get("critical"))))
    lines.extend(["", "### Warnings", ""])
    lines.extend(_markdown_bullets(_strings(alerts.get("warnings"))))
    lines.extend(["", "### Notes", ""])
    lines.extend(_markdown_bullets(_strings(alerts.get("notes"))))
    lines.extend(["", "## 8. Suggested Next Steps", ""])
    lines.extend([f"- {item}" for item in _strings(payload.get("recommended_next_steps"))])
    lines.extend(
        [
            "",
            "## 9. Links",
            "",
            f"- Governance Summary: `{links.get('governance_summary_markdown', '')}`",
            f"- Governance Web View: `{links.get('governance_web_view_html', '')}`",
            f"- Daily Digest Markdown: `{links.get('daily_digest_markdown', '')}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_parameter_governance_daily_digest_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Parameter Governance Daily Digest Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- digest_status: `{payload.get('digest_status')}`",
            f"- summary_level: `{payload.get('summary_level')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- digest_only: `true`",
            "- governance_only: `true`",
            "- apply_executed_by_digest: `false`",
            "- rollback_executed_by_digest: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- digest_json: `{payload.get('digest_json')}`",
            f"- digest_markdown: `{payload.get('digest_markdown')}`",
            "",
        ]
    )


def _resolve_input_paths(
    *,
    as_of: date,
    data_root: Path,
    governance_summary_file: Path | None,
    web_view_metadata_file: Path | None,
    lookback_days: int,
) -> dict[str, Path | None]:
    governance_root = default_governance_root(data_root)
    return {
        "governance_summary": governance_summary_file
        or _latest_dated_artifact(
            root=governance_root,
            prefix="parameter_governance_summary_",
            suffix=".json",
            as_of=as_of,
            lookback_days=lookback_days,
            default_path=default_governance_summary_json_path(data_root, as_of),
        ),
        "web_view_metadata": web_view_metadata_file
        or _latest_dated_artifact(
            root=governance_root / "web",
            prefix="parameter_governance_web_view_",
            suffix=".json",
            as_of=as_of,
            lookback_days=lookback_days,
            default_path=default_web_view_metadata_path(data_root, as_of),
        ),
        "multi_day_review": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "comparison" / "reviews",
            prefix="shadow_vs_production_review_",
            suffix=".json",
            as_of=as_of,
            lookback_days=lookback_days,
            default_path=None,
        ),
        "lifecycle_audit": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "promotion" / "audit",
            prefix="shadow_promotion_lifecycle_audit_",
            suffix=".json",
            as_of=as_of,
            lookback_days=lookback_days,
            default_path=None,
        ),
    }


def _latest_dated_artifact(
    *,
    root: Path,
    prefix: str,
    suffix: str,
    as_of: date,
    lookback_days: int,
    default_path: Path | None,
) -> Path | None:
    if not root.exists():
        return default_path
    earliest = as_of - timedelta(days=lookback_days - 1)
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*{suffix}"):
        raw_date = path.name.removeprefix(prefix).removesuffix(suffix)
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and earliest <= parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _load_governance_summary(path: Path | None) -> tuple[dict[str, Any], str, str]:
    if path is None or not path.exists() or not path.is_file():
        return {}, DIGEST_INPUT_MISSING, f"Governance summary not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, DIGEST_INPUT_INVALID, f"Governance summary JSON is invalid: {exc}"
    except OSError as exc:
        return {}, DIGEST_INPUT_INVALID, f"Governance summary cannot be read: {exc}"
    if not isinstance(payload, dict):
        return {}, DIGEST_INPUT_INVALID, "Governance summary JSON must be an object."
    if payload.get("task_id") != SUMMARY_TASK_ID:
        return payload, DIGEST_INPUT_INVALID, "Governance summary task_id must be TRADING-019."
    return payload, "FOUND", ""


def _summary_safety_validation(
    payload: dict[str, Any],
    *,
    input_status: str,
    input_reason: str,
) -> dict[str, Any]:
    if input_status != "FOUND":
        reason = input_reason or "governance_summary_unavailable"
        return {
            "status": "FAIL",
            "summary_task_id_valid": payload.get("task_id") == SUMMARY_TASK_ID,
            "summary_production_effect_none": False,
            "summary_manual_review_only": False,
            "summary_governance_only": False,
            "summary_apply_not_executed": False,
            "summary_rollback_not_executed": False,
            "summary_broker_execution_false": False,
            "summary_replay_execution_false": False,
            "summary_trading_execution_false": False,
            "summary_no_execution_flags": False,
            "blocking_reasons": [reason],
        }

    checks = {
        "summary_task_id_valid": payload.get("task_id") == SUMMARY_TASK_ID,
        "summary_production_effect_none": (
            payload.get("production_effect") == PRODUCTION_EFFECT_NONE
        ),
        "summary_manual_review_only": payload.get("manual_review_only") is True,
        "summary_governance_only": payload.get("governance_only") is True,
        "summary_apply_not_executed": payload.get("apply_executed_by_governance") is False,
        "summary_rollback_not_executed": payload.get("rollback_executed_by_governance") is False,
        "summary_broker_execution_false": payload.get("broker_execution") is False,
        "summary_replay_execution_false": payload.get("replay_execution") is False,
        "summary_trading_execution_false": payload.get("trading_execution") is False,
    }
    summary_no_execution_flags = all(
        (
            checks["summary_apply_not_executed"],
            checks["summary_rollback_not_executed"],
            checks["summary_broker_execution_false"],
            checks["summary_replay_execution_false"],
            checks["summary_trading_execution_false"],
        )
    )
    checks["summary_no_execution_flags"] = summary_no_execution_flags
    blocking = [key for key, passed in checks.items() if passed is not True]
    return {
        "status": "PASS" if not blocking else "FAIL",
        **checks,
        "blocking_reasons": blocking,
    }


def _digest_status(
    *,
    summary: dict[str, Any],
    input_status: str,
    safety_validation: dict[str, Any],
) -> str:
    if input_status == DIGEST_INPUT_MISSING:
        return DIGEST_INPUT_MISSING
    if input_status == DIGEST_INPUT_INVALID:
        return DIGEST_INPUT_INVALID
    if safety_validation.get("status") != "PASS":
        return DIGEST_SAFETY_BLOCKED

    governance_state = _string_value(summary.get("governance_state"))
    action_level = _string_value(summary.get("action_level"))
    findings = _mapping(summary.get("audit_findings"))
    critical = _strings(findings.get("critical_findings"))
    safety = _mapping(summary.get("safety_boundary_audit"))
    safety_status = _string_value(safety.get("status")) or "MISSING"
    if (
        governance_state == STATE_SAFETY_ANOMALY
        or action_level == ACTION_URGENT
        or bool(critical)
        or safety_status not in {"PASS", ""}
    ):
        return DIGEST_URGENT
    if action_level in {
        ACTION_REVIEW_REQUIRED,
        ACTION_APPROVAL_REQUIRED,
        ACTION_ROLLBACK_REVIEW_REQUIRED,
    }:
        return DIGEST_ACTION_REQUIRED
    if governance_state in {STATE_SHADOW_LEARNING, STATE_APPLIED_NEEDS_MONITORING}:
        return DIGEST_WATCH
    if action_level == ACTION_WATCH:
        return DIGEST_WATCH
    if (
        governance_state in {STATE_SAFE_OBSERVATION, STATE_ROLLBACK_COMPLETED}
        and summary.get("action_required") is not True
    ):
        return DIGEST_OK
    return DIGEST_ACTION_REQUIRED if summary.get("action_required") is True else DIGEST_WATCH


def _summary_level(digest_status: str) -> str:
    return {
        DIGEST_OK: SUMMARY_NORMAL,
        DIGEST_WATCH: SUMMARY_WATCH,
        DIGEST_ACTION_REQUIRED: SUMMARY_ACTION,
        DIGEST_URGENT: SUMMARY_URGENT,
    }.get(digest_status, SUMMARY_UNKNOWN)


def _headline(digest_status: str) -> str:
    return {
        DIGEST_OK: "Parameter governance is stable. No immediate manual action is required.",
        DIGEST_WATCH: "Parameter governance is stable but requires monitoring.",
        DIGEST_ACTION_REQUIRED: (
            "Manual review or approval is required before the next governance step."
        ),
        DIGEST_URGENT: "Safety anomaly detected. Manual inspection is required immediately.",
        DIGEST_INPUT_MISSING: (
            "Governance summary is missing. Digest cannot determine the current state."
        ),
        DIGEST_INPUT_INVALID: "Governance summary is invalid. Digest cannot determine state.",
        DIGEST_SAFETY_BLOCKED: (
            "Digest generation was blocked by invalid governance safety fields."
        ),
        DIGEST_ERROR: "Daily digest generation failed.",
    }.get(digest_status, "Daily digest status is unknown.")


def _recommended_next_steps(digest_status: str) -> list[str]:
    return {
        DIGEST_OK: [
            "Continue observation.",
            "Review the governance web view if you need full details.",
        ],
        DIGEST_WATCH: [
            "Continue monitoring.",
            "Review pending state and latest lifecycle audit.",
        ],
        DIGEST_ACTION_REQUIRED: [
            "Review the pending governance step.",
            "Do not apply or rollback without explicit approval artifacts and danger flags.",
        ],
        DIGEST_URGENT: [
            "Stop relying on automated governance outputs until the anomaly is reviewed.",
            "Inspect the latest lifecycle audit and related artifacts.",
            "Confirm no broker/replay/trading execution occurred unexpectedly.",
        ],
        DIGEST_INPUT_MISSING: [
            "Regenerate or locate the latest TRADING-019 governance summary.",
            "Do not infer governance state from partial artifacts.",
        ],
        DIGEST_INPUT_INVALID: [
            "Regenerate or locate the latest TRADING-019 governance summary.",
            "Do not infer governance state from partial artifacts.",
        ],
        DIGEST_SAFETY_BLOCKED: [
            "Inspect governance summary safety fields.",
            "Do not continue with digest-based decisions until safety fields are fixed.",
        ],
        DIGEST_ERROR: [
            "Inspect the digest run log.",
            "Do not infer governance state from partial artifacts.",
        ],
    }.get(digest_status, ["Inspect the digest artifact."])


def _governance_snapshot(summary: dict[str, Any]) -> dict[str, Any]:
    safety = _mapping(summary.get("safety_boundary_audit"))
    return {
        "governance_state": _string_value(summary.get("governance_state")) or "MISSING",
        "action_required": summary.get("action_required") is True,
        "action_level": _string_value(summary.get("action_level")) or ACTION_NONE,
        "recommended_action": _string_value(summary.get("recommended_action")),
        "safety_boundary_status": _string_value(safety.get("status")) or "MISSING",
    }


def _daily_readout(
    *,
    summary: dict[str, Any],
    digest_status: str,
    pending_items: dict[str, bool],
    alerts: dict[str, list[str]],
) -> dict[str, Any]:
    promotion = _mapping(summary.get("promotion_status"))
    review = _mapping(summary.get("shadow_vs_production_review"))
    safety = _mapping(summary.get("safety_boundary_audit"))
    has_safety_anomaly = (
        digest_status in {DIGEST_URGENT, DIGEST_SAFETY_BLOCKED}
        or _string_value(summary.get("governance_state")) == STATE_SAFETY_ANOMALY
        or _string_value(safety.get("status")) not in {"", "PASS"}
        or bool(alerts.get("critical"))
    )
    return {
        "is_safe": digest_status in {DIGEST_OK, DIGEST_WATCH, DIGEST_ACTION_REQUIRED}
        and not has_safety_anomaly,
        "needs_manual_action": digest_status
        in {
            DIGEST_ACTION_REQUIRED,
            DIGEST_URGENT,
            DIGEST_SAFETY_BLOCKED,
        },
        "has_pending_apply": pending_items.get("pending_apply") is True,
        "has_pending_rollback": pending_items.get("pending_rollback") is True,
        "has_safety_anomaly": has_safety_anomaly,
        "latest_lifecycle_state": _string_value(promotion.get("lifecycle_decision")) or "MISSING",
        "latest_apply_status": _string_value(promotion.get("apply_status")) or "MISSING",
        "latest_rollback_status": _string_value(promotion.get("rollback_status")) or "MISSING",
        "shadow_review_decision": _string_value(review.get("review_decision")) or "MISSING",
    }


def _pending_items(summary: dict[str, Any]) -> dict[str, bool]:
    raw = _mapping(summary.get("pending_items"))
    return {
        "pending_proposal_review": raw.get("pending_proposal_review") is True,
        "pending_preflight": raw.get("pending_preflight") is True,
        "pending_apply": raw.get("pending_apply") is True,
        "pending_rollback": raw.get("pending_rollback") is True,
        "pending_lifecycle_audit": raw.get("pending_lifecycle_audit") is True,
    }


def _weight_snapshot(
    summary: dict[str, Any],
    *,
    alerts: dict[str, list[str]],
) -> dict[str, Any]:
    production = _mapping(summary.get("production_state"))
    shadow = _mapping(summary.get("shadow_state"))
    production_weights = _float_mapping(_mapping(production.get("weights")))
    shadow_weights = _float_mapping(_mapping(shadow.get("weights")))
    delta_from_summary = _float_mapping(_mapping(shadow.get("delta_from_production")))
    delta = (
        delta_from_summary
        if delta_from_summary
        else _weight_delta(production_weights, shadow_weights)
    )
    keys = _ordered_weight_keys(production_weights, shadow_weights, delta)
    delta_summary = [
        {
            "weight_key": key,
            "production": production_weights.get(key),
            "shadow": shadow_weights.get(key),
            "delta": delta.get(key),
        }
        for key in keys
    ]
    largest_key = ""
    largest_value: float | None = None
    if delta:
        largest_key = max(keys, key=lambda key: abs(delta.get(key, 0.0)))
        largest_value = delta.get(largest_key)
    if not production_weights:
        alerts["warnings"].append(
            "Production weights are missing; weight delta summary is limited."
        )
    if not shadow_weights:
        alerts["warnings"].append("Shadow weights are missing; weight delta summary is limited.")
    return {
        "production_weights_available": bool(production_weights),
        "shadow_weights_available": bool(shadow_weights),
        "largest_delta_key": largest_key,
        "largest_delta_value": largest_value,
        "delta_summary": delta_summary,
    }


def _alerts(
    *,
    summary: dict[str, Any],
    digest_status: str,
    input_reason: str,
    safety_validation: dict[str, Any],
) -> dict[str, list[str]]:
    findings = _mapping(summary.get("audit_findings"))
    critical = list(_strings(findings.get("critical_findings")))
    warnings = list(_strings(findings.get("warnings")))
    notes = list(_strings(findings.get("notes")))
    if digest_status == DIGEST_INPUT_MISSING and input_reason:
        warnings.append(input_reason)
    if digest_status == DIGEST_INPUT_INVALID and input_reason:
        critical.append(input_reason)
    if digest_status == DIGEST_SAFETY_BLOCKED:
        critical.append(
            "Digest generation was safety-blocked because governance summary safety fields "
            "are invalid."
        )
        critical.extend(_strings(safety_validation.get("blocking_reasons")))
    return {
        "critical": list(dict.fromkeys(critical)),
        "warnings": list(dict.fromkeys(warnings)),
        "notes": list(dict.fromkeys(notes)),
    }


def _links(
    *,
    summary: dict[str, Any],
    web_metadata: dict[str, Any],
    data_root: Path,
    as_of: date,
    output_md_path: Path,
) -> dict[str, str]:
    outputs = _mapping(summary.get("outputs"))
    output_artifacts = _mapping(web_metadata.get("output_artifacts"))
    html_artifact = _mapping(output_artifacts.get("html"))
    return {
        "governance_summary_markdown": _string_value(outputs.get("markdown"))
        or str(default_governance_summary_json_path(data_root, as_of).with_suffix(".md")),
        "governance_web_view_html": _string_value(html_artifact.get("path"))
        or str(default_web_view_metadata_path(data_root, as_of).with_suffix(".html")),
        "daily_digest_markdown": str(output_md_path),
    }


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    output_artifacts = _mapping(payload.get("output_artifacts"))
    digest_json = _string_value(_mapping(output_artifacts.get("json")).get("path"))
    digest_markdown = _string_value(_mapping(output_artifacts.get("markdown")).get("path"))
    alerts = _mapping(payload.get("alerts"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": _isoformat_z(generated_at),
        "run_status": "PASS" if payload.get("digest_status") != DIGEST_ERROR else DIGEST_ERROR,
        "digest_status": payload.get("digest_status"),
        "summary_level": payload.get("summary_level"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "digest_only": True,
        "governance_only": True,
        "apply_executed_by_digest": False,
        "rollback_executed_by_digest": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "safe_for_scheduler": True,
        "critical_alert_count": len(_strings(alerts.get("critical"))),
        "warning_count": len(_strings(alerts.get("warnings"))),
        "digest_json": digest_json,
        "digest_markdown": digest_markdown,
    }


def _error_payload(
    *,
    as_of: date,
    data_root: Path,
    governance_summary_file: Path | None,
    web_view_metadata_file: Path | None,
    lookback_days: int,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    input_paths = _resolve_input_paths(
        as_of=as_of,
        data_root=data_root,
        governance_summary_file=governance_summary_file,
        web_view_metadata_file=web_view_metadata_file,
        lookback_days=lookback_days,
    )
    input_artifacts = {
        key: _artifact_record(path) for key, path in input_paths.items() if path is not None
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": _isoformat_z(generated_at),
        "lookback_days": lookback_days,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "digest_only": True,
        "governance_only": True,
        "apply_executed_by_digest": False,
        "rollback_executed_by_digest": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "digest_status": DIGEST_ERROR,
        "headline": _headline(DIGEST_ERROR),
        "summary_level": SUMMARY_UNKNOWN,
        "input_artifacts": input_artifacts,
        "governance_snapshot": {
            "governance_state": "ERROR",
            "action_required": True,
            "action_level": ACTION_REVIEW_REQUIRED,
            "recommended_action": "Inspect the digest run log.",
            "safety_boundary_status": "MISSING",
        },
        "daily_readout": {
            "is_safe": False,
            "needs_manual_action": True,
            "has_pending_apply": False,
            "has_pending_rollback": False,
            "has_safety_anomaly": False,
            "latest_lifecycle_state": "MISSING",
            "latest_apply_status": "MISSING",
            "latest_rollback_status": "MISSING",
            "shadow_review_decision": "MISSING",
        },
        "weight_snapshot": {
            "production_weights_available": False,
            "shadow_weights_available": False,
            "largest_delta_key": "",
            "largest_delta_value": None,
            "delta_summary": [],
        },
        "pending_items": {
            "pending_proposal_review": False,
            "pending_preflight": False,
            "pending_apply": False,
            "pending_rollback": False,
            "pending_lifecycle_audit": False,
        },
        "alerts": {"critical": [error], "warnings": [], "notes": []},
        "recommended_next_steps": _recommended_next_steps(DIGEST_ERROR),
        "links": {
            "governance_summary_markdown": str(
                default_governance_summary_json_path(data_root, as_of).with_suffix(".md")
            ),
            "governance_web_view_html": str(
                default_web_view_metadata_path(data_root, as_of).with_suffix(".html")
            ),
            "daily_digest_markdown": str(output_md_path),
        },
        "safety_validation": {
            "status": "FAIL",
            "summary_task_id_valid": False,
            "summary_production_effect_none": False,
            "summary_manual_review_only": False,
            "summary_governance_only": False,
            "summary_apply_not_executed": False,
            "summary_rollback_not_executed": False,
            "summary_broker_execution_false": False,
            "summary_replay_execution_false": False,
            "summary_trading_execution_false": False,
            "summary_no_execution_flags": False,
            "blocking_reasons": [error],
        },
        "output_artifacts": {
            "json": {"path": str(output_json_path)},
            "markdown": {"path": str(output_md_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
        "pipeline_contract": dict(PIPELINE_CONTRACT),
        "audit": {
            "created_by": "scripts/run_parameter_governance_daily_digest.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_digest_artifacts": True,
        },
    }


def _artifact_record(path: Path | None) -> dict[str, Any]:
    exists = path is not None and path.exists() and path.is_file()
    return {
        "status": "FOUND" if exists else "MISSING",
        "path": "" if path is None else str(path),
        "exists": exists,
        "sha256": _sha256(path) if exists and path is not None else "",
        "size_bytes": path.stat().st_size if exists and path is not None else 0,
    }


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _assert_digest_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("parameter governance daily digest production_effect must remain none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("parameter governance daily digest must remain manual_review_only")
    if payload.get("digest_only") is not True:
        raise ValueError("parameter governance daily digest must remain digest_only")
    if payload.get("governance_only") is not True:
        raise ValueError("parameter governance daily digest must remain governance_only")
    if payload.get("apply_executed_by_digest") is not False:
        raise ValueError("parameter governance daily digest must not execute apply")
    if payload.get("rollback_executed_by_digest") is not False:
        raise ValueError("parameter governance daily digest must not execute rollback")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"parameter governance daily digest must keep {field}=false")
    if payload.get("safe_for_scheduler") is not True:
        raise ValueError("parameter governance daily digest should be scheduler-safe")


def _weight_delta(
    production_weights: dict[str, float],
    shadow_weights: dict[str, float],
) -> dict[str, float]:
    if not production_weights or not shadow_weights:
        return {}
    return {
        key: round(shadow_weights.get(key, 0.0) - production_weights.get(key, 0.0), 10)
        for key in _ordered_weight_keys(production_weights, shadow_weights)
    }


def _ordered_weight_keys(*mappings: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for mapping in mappings:
        for key in mapping:
            if key not in seen:
                keys.append(key)
                seen.add(key)
    return keys


def _float_mapping(value: dict[str, Any]) -> dict[str, float]:
    parsed: dict[str, float] = {}
    for key, item in value.items():
        number = _optional_float(item)
        if number is not None:
            parsed[str(key)] = round(number, 10)
    return parsed


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    if isinstance(value, set):
        return [str(item) for item in value if str(item)]
    return []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_float(value: Any) -> float | None:
    try:
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_float(value: Any) -> str:
    parsed = _optional_float(value)
    return "NA" if parsed is None else f"{parsed:.4f}"


def _format_signed_float(value: Any) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "NA"
    sign = "+" if parsed >= 0 else ""
    return f"{sign}{parsed:.4f}"


def _bool_text(value: Any) -> str:
    return str(value is True).lower()


def _markdown_bullets(items: list[str]) -> list[str]:
    return [f"- {item}" for item in items] or ["- None."]
