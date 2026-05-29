from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_text

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "parameter_governance_summary"
RUN_REPORT_TYPE = "parameter_governance_summary_run"
TASK_ID = "TRADING-019"
MODE = "parameter_governance_summary_only"
PRODUCTION_EFFECT_NONE = "none"

STATE_SAFE_OBSERVATION = "SAFE_OBSERVATION"
STATE_SHADOW_LEARNING = "SHADOW_LEARNING"
STATE_SHADOW_REVIEW_READY = "SHADOW_REVIEW_READY"
STATE_PROPOSAL_PENDING_REVIEW = "PROPOSAL_PENDING_REVIEW"
STATE_PREFLIGHT_READY = "PREFLIGHT_READY"
STATE_APPLY_PENDING = "APPLY_PENDING"
STATE_APPLIED_NEEDS_MONITORING = "APPLIED_NEEDS_MONITORING"
STATE_ROLLBACK_COMPLETED = "ROLLBACK_COMPLETED"
STATE_SAFETY_ANOMALY = "SAFETY_ANOMALY"
STATE_INCOMPLETE_DATA = "INCOMPLETE_DATA"
STATE_ERROR = "ERROR"

ACTION_NONE = "NONE"
ACTION_WATCH = "WATCH"
ACTION_REVIEW_REQUIRED = "REVIEW_REQUIRED"
ACTION_APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
ACTION_ROLLBACK_REVIEW_REQUIRED = "ROLLBACK_REVIEW_REQUIRED"
ACTION_URGENT = "URGENT"

REVIEW_DECISION_SHADOW_LOOKS_BETTER = "SHADOW_LOOKS_BETTER"
PROPOSAL_DECISION_MANUAL_REVIEW = "PROPOSE_FOR_MANUAL_REVIEW"
PREFLIGHT_DECISION_PASS = "PASS"
APPLY_DECISION_APPLIED = "APPLIED"
ROLLBACK_DECISION_ROLLED_BACK = "ROLLED_BACK"
LIFECYCLE_DECISION_COMPLETE_WITH_ROLLBACK = "COMPLETE_WITH_ROLLBACK"
LIFECYCLE_DECISION_SAFETY_ANOMALY = "SAFETY_ANOMALY"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"
DEFAULT_LOOKBACK_DAYS = 14

WEIGHT_FIELD_CANDIDATES = ("weights", "base_weights", "production_weights", "target_weights")
WEIGHT_SUM_TOLERANCE = 0.000001


def default_governance_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "governance"


def default_governance_summary_json_path(data_root: Path, as_of: date) -> Path:
    return default_governance_root(data_root) / (
        f"parameter_governance_summary_{as_of.isoformat()}.json"
    )


def default_governance_summary_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_governance_root(data_root)
        / "logs"
        / f"parameter_governance_summary_run_{as_of.isoformat()}.json"
    )


def default_current_shadow_weights_path(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json"


def write_parameter_governance_summary_report(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    shadow_weights_file: Path | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    fail_on_safety_anomaly: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_governance_summary_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_governance_summary_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_parameter_governance_summary_payload(
            as_of=as_of,
            data_root=data_root,
            production_profile_path=production_profile_path,
            shadow_weights_file=shadow_weights_file,
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
            production_profile_path=production_profile_path,
            shadow_weights_file=shadow_weights_file,
            lookback_days=lookback_days,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    _write_json(output_json_path, payload)
    _write_text(output_md_path, render_parameter_governance_summary_report(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_parameter_governance_summary_run_log(run_log))

    if fail_on_safety_anomaly and payload.get("governance_state") == STATE_SAFETY_ANOMALY:
        raise SystemExit(2)
    return payload


def build_parameter_governance_summary_payload(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    shadow_weights_file: Path | None = None,
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
    output_json_path = output_json_path or default_governance_summary_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_governance_summary_run_log_json_path(
        data_root, as_of
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")

    artifact_paths = _resolve_input_artifact_paths(
        as_of=as_of,
        data_root=data_root,
        production_profile_path=production_profile_path,
        shadow_weights_file=shadow_weights_file,
        lookback_days=lookback_days,
    )
    input_artifacts = {
        key: _artifact_record(path, artifact_date=artifact_date)
        for key, (path, artifact_date) in artifact_paths.items()
    }
    payloads = {
        key: (
            _read_structured_object(path)
            if key == "production_profile"
            else _read_json_object(path)
        )
        for key, (path, _artifact_date) in artifact_paths.items()
    }

    findings: dict[str, list[str]] = {"critical_findings": [], "warnings": [], "notes": []}
    production_state = _production_state(
        payload=payloads["production_profile"],
        artifact=input_artifacts["production_profile"],
        findings=findings,
    )
    shadow_state = _shadow_state(
        payload=payloads["current_shadow_weights"],
        artifact=input_artifacts["current_shadow_weights"],
        production_weights=_mapping(production_state.get("weights")),
        findings=findings,
    )
    review_status = _shadow_review_status(payloads["latest_multi_day_review"])
    promotion_status = _promotion_status(
        proposal=payloads["latest_promotion_proposal"],
        preflight=payloads["latest_apply_preflight"],
        apply_result=payloads["latest_apply_result"],
        rollback_result=payloads["latest_rollback_result"],
        lifecycle_audit=payloads["latest_lifecycle_audit"],
    )
    pending_items = _pending_items(
        artifacts=input_artifacts,
        proposal=payloads["latest_promotion_proposal"],
        preflight=payloads["latest_apply_preflight"],
        apply_result=payloads["latest_apply_result"],
        rollback_result=payloads["latest_rollback_result"],
    )
    safety_audit = _safety_boundary_audit(
        payloads=payloads,
        artifacts=input_artifacts,
        findings=findings,
    )
    governance_state = _governance_state(
        production_state=production_state,
        shadow_state=shadow_state,
        review_status=review_status,
        promotion_status=promotion_status,
        pending_items=pending_items,
        findings=findings,
    )
    action_level = _action_level(governance_state=governance_state, findings=findings)
    action_required = action_level not in {ACTION_NONE}
    governance_reason = _governance_reason(
        governance_state=governance_state,
        findings=findings,
        production_state=production_state,
        shadow_state=shadow_state,
        promotion_status=promotion_status,
    )
    recommended_action = _recommended_action(governance_state, action_level)
    _add_state_notes(governance_state=governance_state, findings=findings)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "lookback_days": lookback_days,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "governance_only": True,
        "apply_executed_by_governance": False,
        "rollback_executed_by_governance": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "governance_state": governance_state,
        "governance_reason": governance_reason,
        "action_required": action_required,
        "action_level": action_level,
        "recommended_action": recommended_action,
        "input_artifacts": input_artifacts,
        "production_state": production_state,
        "shadow_state": shadow_state,
        "shadow_vs_production_review": review_status,
        "promotion_status": promotion_status,
        "pending_items": pending_items,
        "safety_boundary_audit": safety_audit,
        "audit_findings": findings,
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "run_log_json": str(run_log_json_path),
            "run_log_markdown": str(run_log_md_path),
        },
        "pipeline_contract": {
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
            "governance_only": True,
        },
        "audit": {
            "created_by": "scripts/run_parameter_governance_summary.py",
            "created_at": generated.isoformat(),
            "read_only": True,
            "no_files_modified_except_governance_artifacts": True,
        },
    }
    _assert_governance_safety_invariants(payload)
    return payload


def render_parameter_governance_summary_report(payload: dict[str, Any]) -> str:
    production = _mapping(payload.get("production_state"))
    shadow = _mapping(payload.get("shadow_state"))
    review = _mapping(payload.get("shadow_vs_production_review"))
    promotion = _mapping(payload.get("promotion_status"))
    pending = _mapping(payload.get("pending_items"))
    safety = _mapping(payload.get("safety_boundary_audit"))
    findings = _mapping(payload.get("audit_findings"))
    production_weights = _mapping(production.get("weights"))
    shadow_weights = _mapping(shadow.get("weights"))
    delta = _mapping(shadow.get("delta_from_production"))

    lines = [f"# Parameter Governance Summary - {payload.get('date')}", ""]
    if payload.get("governance_state") == STATE_SAFETY_ANOMALY:
        lines.extend(["## URGENT: Safety Anomaly Detected", ""])

    lines.extend(
        [
            "## 1. Governance Summary",
            "",
            f"- Governance State: `{payload.get('governance_state')}`",
            f"- Action Required: `{str(payload.get('action_required')).lower()}`",
            f"- Action Level: `{payload.get('action_level')}`",
            f"- Recommended Action: {payload.get('recommended_action')}",
            "- Production Effect: `none`",
            f"- Governance Only: `{str(payload.get('governance_only') is True).lower()}`",
            f"- Broker Execution: `{str(payload.get('broker_execution') is True).lower()}`",
            f"- Replay Execution: `{str(payload.get('replay_execution') is True).lower()}`",
            f"- Trading Execution: `{str(payload.get('trading_execution') is True).lower()}`",
            "",
            "## 2. Production vs Shadow Weights",
            "",
        ]
    )
    if not shadow_weights:
        lines.extend(
            [
                (
                    "Shadow weights not found. Governance state may be SAFE_OBSERVATION or "
                    "INCOMPLETE_DATA depending on available promotion artifacts."
                ),
                "",
            ]
        )
    lines.extend(["| Weight Key | Production | Shadow | Delta |", "|---|---:|---:|---:|"])
    for key in _ordered_weight_keys(production_weights, shadow_weights, delta):
        lines.append(
            "| "
            f"{key} | {_format_float(production_weights.get(key))} | "
            f"{_format_float(shadow_weights.get(key))} | {_format_signed_float(delta.get(key))} |"
        )

    lines.extend(
        [
            "",
            "## 3. Shadow Review Status",
            "",
            f"- Latest multi-day review: `{review.get('status', 'MISSING')}`",
            f"- Review decision: `{review.get('review_decision', 'MISSING')}`",
            f"- Available comparison days: {review.get('available_comparison_days', 0)}",
            f"- Average score delta: {_format_float(review.get('average_score_delta'))}",
            f"- Decision difference count: {review.get('decision_difference_count', 0)}",
            f"- Risk flag delta total: {review.get('risk_flag_delta_total', 0)}",
            "",
            "## 4. Promotion Status",
            "",
            "| Stage | Status | Decision | Executed |",
            "|---|---:|---:|---:|",
            _promotion_row(
                "Proposal",
                promotion.get("proposal_status"),
                promotion.get("proposal_decision"),
                promotion.get("promotion_proposed"),
            ),
            _promotion_row(
                "Preflight",
                promotion.get("preflight_status"),
                promotion.get("preflight_decision"),
                False,
            ),
            _promotion_row(
                "Apply",
                promotion.get("apply_status"),
                promotion.get("apply_decision"),
                promotion.get("apply_executed"),
            ),
            _promotion_row(
                "Rollback",
                promotion.get("rollback_status"),
                promotion.get("rollback_decision"),
                promotion.get("rollback_executed"),
            ),
            _promotion_row(
                "Lifecycle Audit",
                promotion.get("lifecycle_status"),
                promotion.get("lifecycle_decision"),
                False,
            ),
            "",
            "## 5. Pending Items",
            "",
            "| Pending Item | Value |",
            "|---|---:|",
        ]
    )
    for key in (
        "pending_proposal_review",
        "pending_preflight",
        "pending_apply",
        "pending_rollback",
        "pending_lifecycle_audit",
    ):
        lines.append(f"| {key} | `{str(pending.get(key) is True).lower()}` |")

    lines.extend(
        [
            "",
            "## 6. Safety Boundary Audit",
            "",
            "| Check | Status | Notes |",
            "|---|---:|---|",
            _safety_row("broker_execution", payload.get("broker_execution") is not True, "false"),
            _safety_row("replay_execution", payload.get("replay_execution") is not True, "false"),
            _safety_row("trading_execution", payload.get("trading_execution") is not True, "false"),
            _safety_row(
                "lifecycle_anomaly",
                safety.get("latest_lifecycle_has_safety_anomaly") is not True,
                str(safety.get("latest_lifecycle_has_safety_anomaly") is True).lower(),
            ),
            _safety_row(
                "governance_read_only",
                safety.get("production_effect_from_governance") == PRODUCTION_EFFECT_NONE,
                "true",
            ),
            "",
            "## 7. Critical Findings",
            "",
        ]
    )
    critical = _strings(findings.get("critical_findings"))
    lines.extend([f"- {item}" for item in critical] or ["- None."])
    lines.extend(["", "## 8. Warnings", ""])
    warnings = _strings(findings.get("warnings"))
    lines.extend([f"- {item}" for item in warnings] or ["- None."])
    lines.extend(
        [
            "",
            "## 9. Recommended Action",
            "",
            str(payload.get("recommended_action") or "Review governance summary."),
            "",
        ]
    )
    return "\n".join(lines)


def render_parameter_governance_summary_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Parameter Governance Summary Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- governance_state: `{payload.get('governance_state')}`",
            f"- action_level: `{payload.get('action_level')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- governance_only: `true`",
            "- apply_executed_by_governance: `false`",
            "- rollback_executed_by_governance: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- summary_json: `{payload.get('summary_json')}`",
            f"- summary_markdown: `{payload.get('summary_markdown')}`",
            "",
        ]
    )


def _resolve_input_artifact_paths(
    *,
    as_of: date,
    data_root: Path,
    production_profile_path: Path,
    shadow_weights_file: Path | None,
    lookback_days: int,
) -> dict[str, tuple[Path, date | None]]:
    return {
        "production_profile": (production_profile_path, None),
        "current_shadow_weights": (
            shadow_weights_file or default_current_shadow_weights_path(data_root),
            None,
        ),
        "latest_multi_day_review": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "comparison" / "reviews",
            prefix="shadow_vs_production_review_",
            as_of=as_of,
            lookback_days=lookback_days,
        ),
        "latest_promotion_proposal": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "promotion" / "proposals",
            prefix="shadow_promotion_proposal_",
            as_of=as_of,
            lookback_days=lookback_days,
        ),
        "latest_apply_preflight": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "promotion" / "preflight",
            prefix="shadow_promotion_apply_preflight_",
            as_of=as_of,
            lookback_days=lookback_days,
        ),
        "latest_apply_result": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "promotion" / "apply",
            prefix="shadow_promotion_apply_result_",
            as_of=as_of,
            lookback_days=lookback_days,
        ),
        "latest_rollback_result": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "promotion" / "rollback_results",
            prefix="shadow_promotion_rollback_result_",
            as_of=as_of,
            lookback_days=lookback_days,
        ),
        "latest_lifecycle_audit": _latest_dated_artifact(
            root=data_root / "derived" / "weight_iterations" / "promotion" / "audit",
            prefix="shadow_promotion_lifecycle_audit_",
            as_of=as_of,
            lookback_days=lookback_days,
        ),
    }


def _latest_dated_artifact(
    *,
    root: Path,
    prefix: str,
    as_of: date,
    lookback_days: int,
) -> tuple[Path, date | None]:
    default_path = root / f"{prefix}{as_of.isoformat()}.json"
    start = as_of - timedelta(days=lookback_days - 1)
    if not root.exists():
        return default_path, as_of
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*.json"):
        raw_date = path.stem.removeprefix(prefix)
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and start <= parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path, as_of
    latest = max(candidates, key=lambda item: item[0])
    return latest[1], latest[0]


def _production_state(
    *,
    payload: dict[str, Any],
    artifact: dict[str, Any],
    findings: dict[str, list[str]],
) -> dict[str, Any]:
    weight_field, weights = _get_profile_weights(payload)
    weights_sum_valid = _weights_sum_valid(weights)
    if artifact.get("status") != "FOUND" or not weights:
        findings["warnings"].append("production_profile_missing_or_unreadable")
    elif not weights_sum_valid:
        findings["warnings"].append("production_weights_sum_invalid")
    return {
        "status": "AVAILABLE" if weights else "MISSING",
        "profile_path": artifact.get("path", ""),
        "weight_field": weight_field,
        "weights": weights,
        "weights_sum_valid": weights_sum_valid,
        "weight_keys": sorted(weights),
    }


def _shadow_state(
    *,
    payload: dict[str, Any],
    artifact: dict[str, Any],
    production_weights: dict[str, Any],
    findings: dict[str, list[str]],
) -> dict[str, Any]:
    weights = _weights_from_mapping(_mapping(payload.get("weights")))
    weights_sum_valid = _weights_sum_valid(weights)
    production_parsed = _weights_from_mapping(production_weights)
    delta = _weight_delta(production_parsed, weights)
    if artifact.get("status") != "FOUND":
        findings["notes"].append("current_shadow_weights_not_found")
    elif not weights:
        findings["warnings"].append("current_shadow_weights_unreadable")
    elif not weights_sum_valid:
        findings["warnings"].append("shadow_weights_sum_invalid")
    if production_parsed and weights and set(production_parsed) != set(weights):
        findings["warnings"].append("production_shadow_weight_keys_mismatch")
    return {
        "status": "AVAILABLE" if weights else "MISSING",
        "weights": weights,
        "weights_sum_valid": weights_sum_valid,
        "weight_keys": sorted(weights),
        "delta_from_production": delta,
    }


def _shadow_review_status(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "status": "MISSING",
            "review_decision": "MISSING",
            "available_comparison_days": 0,
            "average_score_delta": 0.0,
            "decision_difference_count": 0,
            "risk_flag_delta_total": 0,
        }
    return {
        "status": "AVAILABLE",
        "review_decision": _string_value(payload.get("review_decision")) or "MISSING",
        "available_comparison_days": _int_value(payload.get("available_comparison_days")),
        "average_score_delta": _float_value(payload.get("average_score_delta"), default=0.0),
        "decision_difference_count": _int_value(payload.get("decision_difference_count")),
        "risk_flag_delta_total": _int_value(
            payload.get("shadow_risk_flag_delta_total")
            if "shadow_risk_flag_delta_total" in payload
            else payload.get("risk_flag_delta_total")
        ),
    }


def _promotion_status(
    *,
    proposal: dict[str, Any],
    preflight: dict[str, Any],
    apply_result: dict[str, Any],
    rollback_result: dict[str, Any],
    lifecycle_audit: dict[str, Any],
) -> dict[str, Any]:
    return {
        "proposal_status": "FOUND" if proposal else "MISSING",
        "proposal_decision": _string_value(proposal.get("proposal_decision")) or "MISSING",
        "promotion_proposed": proposal.get("promotion_proposed") is True,
        "preflight_status": "FOUND" if preflight else "MISSING",
        "preflight_decision": _string_value(preflight.get("preflight_decision")) or "MISSING",
        "apply_status": "FOUND" if apply_result else "MISSING",
        "apply_decision": _string_value(apply_result.get("apply_decision")) or "MISSING",
        "apply_executed": apply_result.get("apply_executed") is True,
        "rollback_status": "FOUND" if rollback_result else "MISSING",
        "rollback_decision": _string_value(rollback_result.get("rollback_decision")) or "MISSING",
        "rollback_executed": rollback_result.get("rollback_executed") is True,
        "lifecycle_status": "FOUND" if lifecycle_audit else "MISSING",
        "lifecycle_decision": _string_value(lifecycle_audit.get("lifecycle_decision")) or "MISSING",
    }


def _pending_items(
    *,
    artifacts: dict[str, dict[str, Any]],
    proposal: dict[str, Any],
    preflight: dict[str, Any],
    apply_result: dict[str, Any],
    rollback_result: dict[str, Any],
) -> dict[str, bool]:
    proposal_ready = (
        _string_value(proposal.get("proposal_decision")) == PROPOSAL_DECISION_MANUAL_REVIEW
    )
    preflight_pass = _string_value(preflight.get("preflight_decision")) == PREFLIGHT_DECISION_PASS
    apply_applied = (
        _string_value(apply_result.get("apply_decision")) == APPLY_DECISION_APPLIED
        and apply_result.get("apply_executed") is True
    )
    preflight_missing = not preflight
    apply_missing_or_not_executed = (
        not apply_result or apply_result.get("apply_executed") is not True
    )
    rollback_missing_or_not_executed = (
        not rollback_result or rollback_result.get("rollback_executed") is not True
    )
    latest_lifecycle = artifacts["latest_lifecycle_audit"]
    lifecycle_date = _artifact_date(latest_lifecycle)
    latest_event_dates = [
        _artifact_date(artifacts["latest_apply_result"]),
        _artifact_date(artifacts["latest_rollback_result"]),
    ]
    latest_event_date = max((item for item in latest_event_dates if item is not None), default=None)
    pending_lifecycle = False
    if apply_result or rollback_result:
        pending_lifecycle = not lifecycle_date or (
            latest_event_date is not None and lifecycle_date < latest_event_date
        )
    return {
        "pending_proposal_review": proposal_ready and preflight_missing,
        "pending_preflight": proposal_ready and preflight_missing,
        "pending_apply": preflight_pass and apply_missing_or_not_executed,
        "pending_rollback": apply_applied and rollback_missing_or_not_executed,
        "pending_lifecycle_audit": pending_lifecycle,
    }


def _safety_boundary_audit(
    *,
    payloads: dict[str, dict[str, Any]],
    artifacts: dict[str, dict[str, Any]],
    findings: dict[str, list[str]],
) -> dict[str, Any]:
    blocking: list[str] = []
    for key, payload in payloads.items():
        if key == "production_profile" or not payload:
            continue
        blocking.extend(_artifact_safety_findings(stage=key, payload=payload))
    for reason in dict.fromkeys(blocking):
        findings["critical_findings"].append(reason)

    lifecycle = payloads.get("latest_lifecycle_audit", {})
    latest_lifecycle_has_safety_anomaly = (
        _string_value(lifecycle.get("lifecycle_decision")) == LIFECYCLE_DECISION_SAFETY_ANOMALY
    )
    if latest_lifecycle_has_safety_anomaly:
        findings["critical_findings"].append("lifecycle_audit:lifecycle_decision_safety_anomaly")

    critical = _strings(findings.get("critical_findings"))
    return {
        "status": "FAIL" if critical else "PASS",
        "latest_lifecycle_has_safety_anomaly": latest_lifecycle_has_safety_anomaly,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "production_effect_from_governance": PRODUCTION_EFFECT_NONE,
        "blocking_reasons": list(dict.fromkeys(critical)),
        "artifact_count_scanned": sum(
            1
            for key, artifact in artifacts.items()
            if key != "production_profile" and artifact.get("status") == "FOUND"
        ),
    }


def _artifact_safety_findings(*, stage: str, payload: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is True:
            findings.append(f"{stage}:{field}_true")
    if payload.get("manual_review_only") is False:
        findings.append(f"{stage}:manual_review_only_false")
    if _scheduler_triggered_execution(payload):
        findings.append(f"{stage}:scheduler_triggered_apply_or_rollback")

    production_effect = _string_value(payload.get("production_effect")) or PRODUCTION_EFFECT_NONE
    if (
        stage
        in {
            "current_shadow_weights",
            "latest_multi_day_review",
            "latest_promotion_proposal",
            "latest_apply_preflight",
            "latest_lifecycle_audit",
        }
        and production_effect != PRODUCTION_EFFECT_NONE
    ):
        findings.append(f"{stage}:production_effect_not_none")

    if (
        stage in {"latest_promotion_proposal", "latest_apply_preflight"}
        and payload.get("promotion_executed") is True
    ):
        findings.append(f"{stage}:promotion_executed_true")
    if stage == "latest_apply_preflight" and payload.get("apply_executed") is True:
        findings.append("latest_apply_preflight:apply_executed_true")

    if stage == "latest_apply_result":
        apply_decision = _string_value(payload.get("apply_decision"))
        apply_executed = payload.get("apply_executed") is True
        if apply_executed and apply_decision != APPLY_DECISION_APPLIED:
            findings.append("latest_apply_result:apply_executed_true_but_decision_not_applied")
        if apply_decision == APPLY_DECISION_APPLIED and not _apply_rollback_snapshot_exists(
            payload
        ):
            findings.append("latest_apply_result:applied_but_rollback_snapshot_missing")
        if apply_executed and payload.get("safe_for_scheduler") is True:
            findings.append("latest_apply_result:scheduler_safe_apply_result")
        if not apply_executed and production_effect != PRODUCTION_EFFECT_NONE:
            findings.append("latest_apply_result:non_executed_apply_production_effect_not_none")

    if stage == "latest_rollback_result":
        rollback_decision = _string_value(payload.get("rollback_decision"))
        rollback_executed = payload.get("rollback_executed") is True
        if rollback_executed and rollback_decision != ROLLBACK_DECISION_ROLLED_BACK:
            findings.append(
                "latest_rollback_result:rollback_executed_true_but_decision_not_rolled_back"
            )
        post_rollback = _mapping(payload.get("post_rollback_validation"))
        if rollback_executed and post_rollback.get("status") != "PASS":
            findings.append("latest_rollback_result:post_rollback_validation_not_pass")
        if rollback_executed and payload.get("safe_for_scheduler") is True:
            findings.append("latest_rollback_result:scheduler_safe_rollback_result")
        if not rollback_executed and production_effect != PRODUCTION_EFFECT_NONE:
            findings.append(
                "latest_rollback_result:non_executed_rollback_production_effect_not_none"
            )
    return findings


def _governance_state(
    *,
    production_state: dict[str, Any],
    shadow_state: dict[str, Any],
    review_status: dict[str, Any],
    promotion_status: dict[str, Any],
    pending_items: dict[str, bool],
    findings: dict[str, list[str]],
) -> str:
    if findings["critical_findings"]:
        return STATE_SAFETY_ANOMALY
    if production_state.get("status") != "AVAILABLE":
        return STATE_INCOMPLETE_DATA

    if (
        promotion_status.get("rollback_decision") == ROLLBACK_DECISION_ROLLED_BACK
        and promotion_status.get("rollback_executed") is True
        and promotion_status.get("lifecycle_decision") == LIFECYCLE_DECISION_COMPLETE_WITH_ROLLBACK
    ):
        return STATE_ROLLBACK_COMPLETED
    if (
        promotion_status.get("apply_decision") == APPLY_DECISION_APPLIED
        and promotion_status.get("apply_executed") is True
        and promotion_status.get("rollback_executed") is not True
    ):
        return STATE_APPLIED_NEEDS_MONITORING
    if (
        promotion_status.get("preflight_decision") == PREFLIGHT_DECISION_PASS
        and promotion_status.get("apply_executed") is not True
    ):
        return STATE_PREFLIGHT_READY
    if (
        promotion_status.get("proposal_decision") == PROPOSAL_DECISION_MANUAL_REVIEW
        and promotion_status.get("preflight_decision") != PREFLIGHT_DECISION_PASS
    ):
        return STATE_PROPOSAL_PENDING_REVIEW
    if (
        review_status.get("review_decision") == REVIEW_DECISION_SHADOW_LOOKS_BETTER
        and promotion_status.get("proposal_status") != "FOUND"
    ):
        return STATE_SHADOW_REVIEW_READY
    if shadow_state.get("status") == "AVAILABLE" and not any(pending_items.values()):
        return STATE_SHADOW_LEARNING
    if not any(pending_items.values()):
        return STATE_SAFE_OBSERVATION
    return STATE_INCOMPLETE_DATA


def _action_level(*, governance_state: str, findings: dict[str, list[str]]) -> str:
    if governance_state == STATE_SAFETY_ANOMALY:
        return ACTION_URGENT
    if governance_state in {STATE_SAFE_OBSERVATION, STATE_ROLLBACK_COMPLETED}:
        return ACTION_NONE
    if governance_state == STATE_SHADOW_LEARNING:
        return ACTION_WATCH
    if governance_state in {STATE_SHADOW_REVIEW_READY, STATE_PROPOSAL_PENDING_REVIEW}:
        return ACTION_REVIEW_REQUIRED
    if governance_state in {STATE_PREFLIGHT_READY, STATE_APPLY_PENDING}:
        return ACTION_APPROVAL_REQUIRED
    if governance_state == STATE_APPLIED_NEEDS_MONITORING:
        return ACTION_ROLLBACK_REVIEW_REQUIRED if findings.get("warnings") else ACTION_WATCH
    return ACTION_REVIEW_REQUIRED


def _governance_reason(
    *,
    governance_state: str,
    findings: dict[str, list[str]],
    production_state: dict[str, Any],
    shadow_state: dict[str, Any],
    promotion_status: dict[str, Any],
) -> str:
    if governance_state == STATE_SAFETY_ANOMALY:
        return "; ".join(findings["critical_findings"]) or "Safety anomaly detected."
    if governance_state == STATE_INCOMPLETE_DATA:
        return "Production profile or critical governance artifacts are missing or unreadable."
    if governance_state == STATE_ROLLBACK_COMPLETED:
        return "Latest rollback result and lifecycle audit indicate rollback completed."
    if governance_state == STATE_APPLIED_NEEDS_MONITORING:
        return "Latest apply result is APPLIED and no successful rollback result is present."
    if governance_state == STATE_PREFLIGHT_READY:
        return "Latest preflight PASS exists and apply has not executed."
    if governance_state == STATE_PROPOSAL_PENDING_REVIEW:
        return "Latest proposal is waiting for manual review or preflight."
    if governance_state == STATE_SHADOW_REVIEW_READY:
        return "Latest multi-day review says shadow looks better and no proposal is present."
    if governance_state == STATE_SHADOW_LEARNING:
        return "Current shadow weights are readable and no promotion lifecycle action is pending."
    if production_state.get("status") == "AVAILABLE" and shadow_state.get("status") == "MISSING":
        return "Production state is readable and no promotion lifecycle action is pending."
    return "Shadow and production states are readable and no safety anomaly was detected."


def _recommended_action(governance_state: str, action_level: str) -> str:
    if action_level == ACTION_URGENT:
        return "Investigate safety anomaly before using governance conclusions."
    if governance_state == STATE_SHADOW_LEARNING:
        return "Continue shadow learning and collect comparison evidence."
    if governance_state == STATE_SHADOW_REVIEW_READY:
        return "Review multi-day shadow evidence and decide whether to create a proposal."
    if governance_state == STATE_PROPOSAL_PENDING_REVIEW:
        return "Complete manual proposal review before any preflight."
    if governance_state == STATE_PREFLIGHT_READY:
        return "Manual approval is required before explicit apply."
    if governance_state == STATE_APPLIED_NEEDS_MONITORING:
        return "Monitor post-apply behavior and review whether rollback is needed."
    if governance_state == STATE_INCOMPLETE_DATA:
        return "Inspect missing or unreadable governance artifacts."
    return "Continue observation."


def _add_state_notes(*, governance_state: str, findings: dict[str, list[str]]) -> None:
    if governance_state == STATE_ROLLBACK_COMPLETED:
        findings["notes"].append(
            "Latest lifecycle audit indicates rollback completed successfully."
        )
    if governance_state == STATE_SAFE_OBSERVATION:
        findings["notes"].append("No pending governance action was detected.")


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("outputs"))
    findings = _mapping(payload.get("audit_findings"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": generated_at.isoformat(),
        "run_status": (
            "SAFETY_ANOMALY" if payload.get("governance_state") == STATE_SAFETY_ANOMALY else "PASS"
        ),
        "governance_state": payload.get("governance_state"),
        "action_level": payload.get("action_level"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "governance_only": True,
        "apply_executed_by_governance": False,
        "rollback_executed_by_governance": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "safe_for_scheduler": True,
        "safety_boundary_status": _mapping(payload.get("safety_boundary_audit")).get("status"),
        "critical_findings_count": len(_strings(findings.get("critical_findings"))),
        "warnings_count": len(_strings(findings.get("warnings"))),
        "summary_json": outputs.get("json"),
        "summary_markdown": outputs.get("markdown"),
    }


def _error_payload(
    *,
    as_of: date,
    data_root: Path,
    production_profile_path: Path,
    shadow_weights_file: Path | None,
    lookback_days: int,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    artifact_paths = _resolve_input_artifact_paths(
        as_of=as_of,
        data_root=data_root,
        production_profile_path=production_profile_path,
        shadow_weights_file=shadow_weights_file,
        lookback_days=lookback_days,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "lookback_days": lookback_days,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "governance_only": True,
        "apply_executed_by_governance": False,
        "rollback_executed_by_governance": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "governance_state": STATE_ERROR,
        "governance_reason": error,
        "action_required": True,
        "action_level": ACTION_REVIEW_REQUIRED,
        "recommended_action": "Inspect governance summary runtime error.",
        "input_artifacts": {
            key: _artifact_record(path, artifact_date=artifact_date)
            for key, (path, artifact_date) in artifact_paths.items()
        },
        "production_state": {"status": "MISSING", "weights": {}, "weights_sum_valid": False},
        "shadow_state": {"status": "MISSING", "weights": {}, "weights_sum_valid": False},
        "shadow_vs_production_review": {"status": "MISSING", "review_decision": "MISSING"},
        "promotion_status": _promotion_status(
            proposal={},
            preflight={},
            apply_result={},
            rollback_result={},
            lifecycle_audit={},
        ),
        "pending_items": {
            "pending_proposal_review": False,
            "pending_preflight": False,
            "pending_apply": False,
            "pending_rollback": False,
            "pending_lifecycle_audit": False,
        },
        "safety_boundary_audit": {
            "status": "FAIL",
            "latest_lifecycle_has_safety_anomaly": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "production_effect_from_governance": PRODUCTION_EFFECT_NONE,
            "blocking_reasons": [error],
        },
        "audit_findings": {"critical_findings": [], "warnings": [], "notes": [error]},
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "run_log_json": str(run_log_json_path),
            "run_log_markdown": str(run_log_md_path),
        },
        "audit": {
            "created_by": "scripts/run_parameter_governance_summary.py",
            "created_at": generated_at.isoformat(),
            "read_only": True,
            "no_files_modified_except_governance_artifacts": True,
        },
    }


def _get_profile_weights(profile: dict[str, Any]) -> tuple[str, dict[str, float]]:
    for field in WEIGHT_FIELD_CANDIDATES:
        weights = _weights_from_mapping(_mapping(profile.get(field)))
        if weights:
            return field, _rounded_weights(weights)
    return "", {}


def _apply_rollback_snapshot_exists(payload: dict[str, Any]) -> bool:
    rollback = _mapping(payload.get("rollback"))
    snapshot_created = rollback.get("snapshot_created") is True
    snapshot_sha = _string_value(rollback.get("snapshot_file_sha256")) or _string_value(
        rollback.get("snapshot_sha256")
    )
    snapshot_path_value = _string_value(rollback.get("snapshot_path"))
    if not snapshot_created or not snapshot_sha:
        return False
    if not snapshot_path_value:
        return False
    return Path(snapshot_path_value).exists()


def _scheduler_triggered_execution(payload: dict[str, Any]) -> bool:
    suspicious_keys = {
        "scheduler_triggered_apply",
        "scheduler_triggered_rollback",
        "apply_triggered_by_scheduler",
        "rollback_triggered_by_scheduler",
        "scheduler_apply_executed",
        "scheduler_rollback_executed",
    }
    for key, value in _walk_items(payload):
        normalized = key.lower()
        if normalized in suspicious_keys and value is True:
            return True
    return False


def _walk_items(value: Any) -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            found.append((str(key), item))
            found.extend(_walk_items(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_walk_items(item))
    return found


def _artifact_record(path: Path, *, artifact_date: date | None = None) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "status": "FOUND" if exists else "MISSING",
        "path": str(path),
        "exists": exists,
        "date": artifact_date.isoformat() if exists and artifact_date is not None else "",
        "sha256": _sha256(path) if exists else "",
        "size_bytes": path.stat().st_size if exists else 0,
    }


def _artifact_date(artifact: dict[str, Any]) -> date | None:
    return _parse_iso_date(_string_value(artifact.get("date")))


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_structured_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            payload = json.loads(text)
        else:
            payload = safe_load_yaml_text(text) or {}
    except (OSError, json.JSONDecodeError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _assert_governance_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("parameter governance summary production_effect must remain none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("parameter governance summary must remain manual_review_only")
    if payload.get("governance_only") is not True:
        raise ValueError("parameter governance summary must remain governance_only")
    if payload.get("apply_executed_by_governance") is not False:
        raise ValueError("parameter governance summary must not execute apply")
    if payload.get("rollback_executed_by_governance") is not False:
        raise ValueError("parameter governance summary must not execute rollback")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"parameter governance summary must keep {field}=false")
    if payload.get("safe_for_scheduler") is not True:
        raise ValueError("parameter governance summary should be scheduler-safe")


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        parsed = _optional_float(value)
        if parsed is not None:
            weights[str(key)] = parsed
    return _rounded_weights(weights)


def _rounded_weights(weights: dict[str, Any]) -> dict[str, float]:
    return {
        key: round(_float_value(value, default=0.0), 10) for key, value in sorted(weights.items())
    }


def _weights_sum_valid(weights: dict[str, float]) -> bool:
    return bool(weights) and abs(sum(weights.values()) - 1.0) <= WEIGHT_SUM_TOLERANCE


def _weight_delta(
    production_weights: dict[str, float],
    shadow_weights: dict[str, float],
) -> dict[str, float]:
    if not production_weights or not shadow_weights:
        return {}
    return {
        key: round(shadow_weights.get(key, 0.0) - production_weights.get(key, 0.0), 10)
        for key in sorted(set(production_weights) | set(shadow_weights))
    }


def _ordered_weight_keys(*mappings: dict[str, Any]) -> list[str]:
    keys: set[str] = set()
    for mapping in mappings:
        keys.update(mapping.keys())
    return sorted(keys)


def _promotion_row(stage: str, status: Any, decision: Any, executed: Any) -> str:
    executed_text = str(executed is True).lower()
    return (
        f"| {stage} | `{status or 'MISSING'}` | `{decision or 'MISSING'}` | " f"`{executed_text}` |"
    )


def _safety_row(check: str, passed: bool, notes: str) -> str:
    return f"| {check} | `{'PASS' if passed else 'FAIL'}` | {notes} |"


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _float_value(value: Any, *, default: float) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed


def _int_value(value: Any) -> int:
    try:
        if isinstance(value, bool):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


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
