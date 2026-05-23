from __future__ import annotations

import glob
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "pipeline_health_summary"
RUN_REPORT_TYPE = "pipeline_health_summary_run"
TASK_ID = "TRADING-023"
MODE = "pipeline_health_summary_only"
PRODUCTION_EFFECT_NONE = "none"
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_FRESHNESS_DAYS = 2

HEALTH_OK = "OK"
HEALTH_WATCH = "WATCH"
HEALTH_ACTION_REQUIRED = "ACTION_REQUIRED"
HEALTH_CRITICAL = "CRITICAL"
HEALTH_INCOMPLETE = "INCOMPLETE"
HEALTH_ERROR = "ERROR"

STATUS_HEALTHY = "HEALTHY"
STATUS_WATCH = "WATCH"
STATUS_ACTION_REQUIRED = "ACTION_REQUIRED"
STATUS_CRITICAL = "CRITICAL"
STATUS_MISSING = "MISSING"
STATUS_STALE = "STALE"
STATUS_OPTIONAL_MISSING = "OPTIONAL_MISSING"
STATUS_UNKNOWN = "UNKNOWN"
STATUS_ERROR = "ERROR"

ARTIFACT_FOUND = "FOUND"
ARTIFACT_MISSING = "MISSING"
FRESHNESS_FRESH = "FRESH"
FRESHNESS_STALE = "STALE"
FRESHNESS_UNKNOWN = "UNKNOWN"

SUMMARY_NORMAL = "NORMAL"
SUMMARY_WATCH = "WATCH"
SUMMARY_ACTION = "ACTION"
SUMMARY_CRITICAL = "CRITICAL"
SUMMARY_UNKNOWN = "UNKNOWN"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"
DATE_PATTERN = re.compile(r"(?P<year>\d{4})[-_](?P<month>\d{2})[-_](?P<day>\d{2})")

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
    "runs_operator_brief_script": False,
    "runs_pipeline_health_summary_script": False,
    "runs_market_pipeline": False,
    "runs_backtest_pipeline": False,
    "runs_scoring_pipeline": False,
    "runs_broker_runner": False,
    "runs_paper_runner": False,
    "runs_replay_runner": False,
    "writes_production_profile": False,
    "writes_production_weights": False,
    "writes_shadow_weights": False,
    "writes_approved_profile": False,
    "promotes_shadow_to_production": False,
    "triggers_trade": False,
    "production_effect": PRODUCTION_EFFECT_NONE,
    "manual_review_only": True,
    "pipeline_health_only": True,
    "read_only": True,
}


@dataclass(frozen=True)
class PipelineDefinition:
    pipeline_id: str
    name: str
    category: str
    required: bool
    expected_artifact_glob: str
    status_field: str
    healthy_values: tuple[str, ...]
    action_values: tuple[str, ...]
    critical_values: tuple[str, ...]
    warning_values: tuple[str, ...] = ("WATCH", "WARNING", "PASS_WITH_WARNINGS")
    stale_after_days: int | None = None
    allow_apply_execution: bool = False
    allow_rollback_execution: bool = False
    allow_promotion_execution: bool = False
    allowed_production_effects: tuple[str, ...] = (PRODUCTION_EFFECT_NONE,)


DEFAULT_PIPELINE_REGISTRY: tuple[PipelineDefinition, ...] = (
    PipelineDefinition(
        pipeline_id="TRADING-018B",
        name="Daily Shadow Weight Iteration",
        category="weight_iteration",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/shadow/candidates/" "shadow_weight_candidate_*.json"
        ),
        status_field="decision",
        healthy_values=("UPDATE", "NO_UPDATE"),
        action_values=("INSUFFICIENT_DATA",),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-018C",
        name="Shadow vs Production Comparison",
        category="weight_iteration",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/comparison/" "daily_shadow_vs_production_*.json"
        ),
        status_field="comparison_status",
        healthy_values=("AVAILABLE", "PASS", "OK"),
        action_values=("INSUFFICIENT_DATA", "INPUT_MISSING", "INPUT_INVALID"),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-018C2",
        name="Shadow vs Production Multi-day Review",
        category="weight_iteration",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/comparison/reviews/"
            "shadow_vs_production_review_*.json"
        ),
        status_field="review_decision",
        healthy_values=(
            "CONTINUE_OBSERVATION",
            "SHADOW_LOOKS_BETTER",
            "SHADOW_LOOKS_WORSE",
        ),
        action_values=("INSUFFICIENT_HISTORY",),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-018D",
        name="Shadow Promotion Proposal",
        category="promotion",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/promotion/proposals/" "shadow_promotion_proposal_*.json"
        ),
        status_field="proposal_decision",
        healthy_values=("CONTINUE_OBSERVATION", "REJECT_SHADOW"),
        action_values=(
            "PROPOSE_FOR_MANUAL_REVIEW",
            "INSUFFICIENT_HISTORY",
            "INSUFFICIENT_DATA",
        ),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-018E1",
        name="Shadow Promotion Apply Preflight",
        category="promotion",
        required=False,
        expected_artifact_glob=(
            "data/derived/weight_iterations/promotion/preflight/"
            "shadow_promotion_apply_preflight_*.json"
        ),
        status_field="preflight_decision",
        healthy_values=("PASS",),
        warning_values=("WARNING",),
        action_values=(
            "INSUFFICIENT_DATA",
            "APPROVAL_INVALID",
            "PROPOSAL_INVALID",
            "WEIGHT_MISMATCH",
            "TARGET_PROFILE_MISMATCH",
        ),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
        stale_after_days=30,
    ),
    PipelineDefinition(
        pipeline_id="TRADING-018E2",
        name="Shadow Promotion Apply Result",
        category="promotion",
        required=False,
        expected_artifact_glob=(
            "data/derived/weight_iterations/promotion/apply/" "shadow_promotion_apply_result_*.json"
        ),
        status_field="apply_decision",
        healthy_values=("APPLIED",),
        action_values=(
            "INSUFFICIENT_DATA",
            "APPROVAL_INVALID",
            "PREFLIGHT_INVALID",
            "DANGER_FLAG_MISSING",
            "TARGET_PROFILE_CHANGED",
            "TARGET_PROFILE_MISMATCH",
            "ROLLBACK_SNAPSHOT_FAILED",
        ),
        critical_values=(
            "WRITE_FAILED",
            "POST_APPLY_VALIDATION_FAILED",
            "SAFETY_BLOCKED",
            "ERROR",
        ),
        stale_after_days=30,
        allow_apply_execution=True,
        allow_promotion_execution=True,
        allowed_production_effects=(
            PRODUCTION_EFFECT_NONE,
            "profile_updated_only_if_apply_executed",
        ),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-018E3",
        name="Shadow Promotion Rollback Result",
        category="promotion",
        required=False,
        expected_artifact_glob=(
            "data/derived/weight_iterations/promotion/rollback_results/"
            "shadow_promotion_rollback_result_*.json"
        ),
        status_field="rollback_decision",
        healthy_values=("ROLLED_BACK",),
        action_values=(
            "INSUFFICIENT_DATA",
            "APPROVAL_INVALID",
            "APPLY_RESULT_INVALID",
            "DANGER_FLAG_MISSING",
            "ROLLBACK_SNAPSHOT_INVALID",
            "TARGET_PROFILE_CHANGED",
            "TARGET_PROFILE_MISMATCH",
        ),
        critical_values=(
            "CURRENT_SNAPSHOT_FAILED",
            "WRITE_FAILED",
            "POST_ROLLBACK_VALIDATION_FAILED",
            "SAFETY_BLOCKED",
            "ERROR",
        ),
        stale_after_days=30,
        allow_rollback_execution=True,
        allowed_production_effects=(
            PRODUCTION_EFFECT_NONE,
            "profile_rolled_back_only_if_rollback_executed",
        ),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-018F",
        name="Shadow Promotion Lifecycle Audit",
        category="promotion",
        required=False,
        expected_artifact_glob=(
            "data/derived/weight_iterations/promotion/audit/"
            "shadow_promotion_lifecycle_audit_*.json"
        ),
        status_field="lifecycle_decision",
        healthy_values=(
            "COMPLETE_WITH_ROLLBACK",
            "COMPLETE_APPLIED_NO_ROLLBACK",
            "PROPOSAL_ONLY",
            "PREFLIGHT_ONLY",
        ),
        action_values=(
            "APPLY_FAILED_OR_BLOCKED",
            "ROLLBACK_FAILED_OR_BLOCKED",
            "INCOMPLETE_ARTIFACTS",
        ),
        critical_values=("SAFETY_ANOMALY", "ERROR"),
        stale_after_days=30,
    ),
    PipelineDefinition(
        pipeline_id="TRADING-019",
        name="Parameter Governance Summary",
        category="governance",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/governance/" "parameter_governance_summary_*.json"
        ),
        status_field="governance_state",
        healthy_values=(
            "SAFE_OBSERVATION",
            "SHADOW_LEARNING",
            "SHADOW_REVIEW_READY",
            "APPLIED_NEEDS_MONITORING",
            "ROLLBACK_COMPLETED",
        ),
        action_values=(
            "PROPOSAL_PENDING_REVIEW",
            "PREFLIGHT_READY",
            "APPLY_PENDING",
            "INCOMPLETE_DATA",
        ),
        critical_values=("SAFETY_ANOMALY", "ERROR"),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-020",
        name="Parameter Governance Web View",
        category="governance",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/governance/web/" "parameter_governance_web_view_*.json"
        ),
        status_field="render_decision",
        healthy_values=("RENDERED",),
        action_values=("INPUT_MISSING", "INPUT_INVALID"),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-021",
        name="Parameter Governance Daily Digest",
        category="governance",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/governance/digests/"
            "parameter_governance_daily_digest_*.json"
        ),
        status_field="digest_status",
        healthy_values=("OK",),
        warning_values=("WATCH",),
        action_values=("ACTION_REQUIRED", "INPUT_MISSING", "INPUT_INVALID"),
        critical_values=("URGENT", "SAFETY_BLOCKED", "ERROR"),
    ),
    PipelineDefinition(
        pipeline_id="TRADING-022",
        name="Daily Trading System Operator Brief",
        category="operator",
        required=True,
        expected_artifact_glob=(
            "data/derived/operator_briefs/" "daily_trading_system_operator_brief_*.json"
        ),
        status_field="brief_status",
        healthy_values=("OK",),
        warning_values=("WATCH",),
        action_values=("ACTION_REQUIRED", "INPUT_MISSING", "INPUT_INVALID"),
        critical_values=("URGENT", "SAFETY_BLOCKED", "ERROR"),
    ),
)


def default_pipeline_health_root(data_root: Path) -> Path:
    return data_root / "derived" / "pipeline_health"


def default_pipeline_health_json_path(data_root: Path, as_of: date) -> Path:
    return default_pipeline_health_root(data_root) / (
        f"pipeline_health_summary_{as_of.isoformat()}.json"
    )


def default_pipeline_health_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_pipeline_health_root(data_root)
        / "logs"
        / f"pipeline_health_summary_run_{as_of.isoformat()}.json"
    )


def write_pipeline_health_summary(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    fail_on_critical: bool = False,
    include_optional_pipelines: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
    registry: tuple[PipelineDefinition, ...] = DEFAULT_PIPELINE_REGISTRY,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_pipeline_health_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_pipeline_health_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_pipeline_health_summary_payload(
            as_of=as_of,
            data_root=data_root,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
            include_optional_pipelines=include_optional_pipelines,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            registry=registry,
        )
    except Exception as exc:  # pragma: no cover - defensive report path
        payload = _error_payload(
            as_of=as_of,
            data_root=data_root,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
            include_optional_pipelines=include_optional_pipelines,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    _write_json(output_json_path, payload)
    _write_text(output_md_path, render_pipeline_health_summary_markdown(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_pipeline_health_summary_run_log(run_log))

    if fail_on_critical and payload.get("health_status") == HEALTH_CRITICAL:
        raise SystemExit(2)
    return payload


def build_pipeline_health_summary_payload(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    include_optional_pipelines: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
    registry: tuple[PipelineDefinition, ...] = DEFAULT_PIPELINE_REGISTRY,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if freshness_days < 0:
        raise ValueError("freshness_days must be non-negative")

    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_pipeline_health_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_pipeline_health_run_log_json_path(
        data_root, as_of
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")
    included_registry = tuple(
        definition for definition in registry if definition.required or include_optional_pipelines
    )

    pipeline_results = [
        _scan_pipeline(
            definition=definition,
            as_of=as_of,
            data_root=data_root,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
        )
        for definition in included_registry
    ]
    missing_required = _issue_records(
        pipeline_results,
        lambda item: item["required"] is True and item["status"] == STATUS_MISSING,
        "Required artifact is missing.",
    )
    stale_pipelines = _issue_records(
        pipeline_results,
        lambda item: item["freshness_status"] == FRESHNESS_STALE,
        "Artifact is older than freshness threshold.",
    )
    critical_pipelines = _issue_records(
        pipeline_results,
        lambda item: item["status"] == STATUS_CRITICAL,
        "Critical pipeline condition detected.",
    )
    warning_pipelines = _issue_records(
        pipeline_results,
        _is_warning_result,
        "Pipeline requires attention or has a non-blocking warning.",
    )
    health_status = _overall_health_status(pipeline_results)
    alerts = _alerts(
        health_status=health_status,
        critical_pipelines=critical_pipelines,
        warning_pipelines=warning_pipelines,
    )
    safety_validation = _safety_validation(critical_pipelines)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": _isoformat_z(generated),
        "lookback_days": lookback_days,
        "freshness_days": freshness_days,
        "include_optional_pipelines": include_optional_pipelines,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "pipeline_health_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "pipelines_executed_by_health_check": False,
        "apply_executed_by_health_check": False,
        "rollback_executed_by_health_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "health_status": health_status,
        "summary_level": _summary_level(health_status),
        "headline": _headline(health_status),
        "coverage": {
            "registered_pipelines": len(pipeline_results),
            "required_pipelines": sum(1 for item in pipeline_results if item["required"]),
            "available_pipelines": sum(
                1 for item in pipeline_results if item["artifact_status"] == ARTIFACT_FOUND
            ),
            "missing_required_pipelines": len(missing_required),
            "stale_required_pipelines": sum(
                1
                for item in pipeline_results
                if item["required"] is True and item["freshness_status"] == FRESHNESS_STALE
            ),
            "critical_pipelines": len(critical_pipelines),
            "warning_pipelines": len(warning_pipelines),
        },
        "pipeline_results": pipeline_results,
        "missing_required_pipelines": missing_required,
        "stale_pipelines": stale_pipelines,
        "critical_pipelines": critical_pipelines,
        "warning_pipelines": warning_pipelines,
        "operator_brief_integration": _operator_brief_integration(health_status),
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(health_status),
        "safety_validation": safety_validation,
        "output_artifacts": {
            "json": {"path": str(output_json_path)},
            "markdown": {"path": str(output_md_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
        "pipeline_contract": dict(PIPELINE_CONTRACT),
        "audit": {
            "created_by": "scripts/run_pipeline_health_summary.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_pipeline_health_artifacts": True,
            "registry_total_pipelines": len(registry),
        },
    }
    _assert_pipeline_health_safety_invariants(payload)
    return payload


def render_pipeline_health_summary_markdown(payload: dict[str, Any]) -> str:
    health_status = _string_value(payload.get("health_status")) or HEALTH_ERROR
    coverage = _mapping(payload.get("coverage"))
    required = [
        item for item in _mappings(payload.get("pipeline_results")) if item.get("required") is True
    ]
    optional = [
        item
        for item in _mappings(payload.get("pipeline_results"))
        if item.get("required") is not True
    ]
    alerts = _mapping(payload.get("alerts"))

    lines = [f"# Pipeline Health Summary - {payload.get('date')}", ""]
    if health_status == HEALTH_CRITICAL:
        lines.extend(["## CRITICAL: Pipeline Health Issue Detected", ""])
    elif health_status == HEALTH_ACTION_REQUIRED:
        lines.extend(["## Action Required", ""])

    lines.extend(
        [
            "## 1. Health Summary",
            "",
            f"- Health Status: `{health_status}`",
            f"- Summary Level: `{payload.get('summary_level', SUMMARY_UNKNOWN)}`",
            f"- Headline: {payload.get('headline') or ''}",
            f"- Registered Pipelines: `{coverage.get('registered_pipelines', 0)}`",
            f"- Required Pipelines: `{coverage.get('required_pipelines', 0)}`",
            ("- Missing Required Pipelines: " f"`{coverage.get('missing_required_pipelines', 0)}`"),
            ("- Stale Required Pipelines: " f"`{coverage.get('stale_required_pipelines', 0)}`"),
            f"- Critical Pipelines: `{coverage.get('critical_pipelines', 0)}`",
            f"- Warning Pipelines: `{coverage.get('warning_pipelines', 0)}`",
            "",
            "## 2. Required Pipelines",
            "",
            "| Pipeline | Status | Freshness | Decision | Artifact |",
            "|---|---:|---:|---:|---|",
        ]
    )
    lines.extend(_pipeline_table_rows(required))
    lines.extend(
        [
            "",
            "## 3. Optional Pipelines",
            "",
            "| Pipeline | Status | Freshness | Decision | Artifact |",
            "|---|---:|---:|---:|---|",
        ]
    )
    if optional:
        lines.extend(_pipeline_table_rows(optional))
    else:
        lines.append("| None | - | - | - | - |")

    lines.extend(["", "## 4. Critical Alerts", ""])
    lines.extend(_markdown_bullets(_strings(alerts.get("critical"))))
    lines.extend(["", "## 5. Warnings", ""])
    lines.extend(_markdown_bullets(_strings(alerts.get("warnings"))))
    lines.extend(["", "## 6. Recommended Next Steps", ""])
    lines.extend(_markdown_bullets(_strings(payload.get("recommended_next_steps"))))
    lines.extend(
        [
            "",
            "## 7. Safety Statement",
            "",
            (
                "This task is read-only and did not execute any pipeline, broker, "
                "replay, or trading process."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def render_pipeline_health_summary_run_log(run_log: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Pipeline Health Summary Run Log - {run_log.get('date')}",
            "",
            f"- Run Status: `{run_log.get('run_status')}`",
            f"- Health Status: `{run_log.get('health_status')}`",
            f"- Summary Level: `{run_log.get('summary_level')}`",
            f"- Registered Pipelines: `{run_log.get('registered_pipelines')}`",
            f"- Missing Required Pipelines: `{run_log.get('missing_required_pipelines')}`",
            f"- Stale Required Pipelines: `{run_log.get('stale_required_pipelines')}`",
            f"- Critical Pipelines: `{run_log.get('critical_pipelines')}`",
            f"- Warning Pipelines: `{run_log.get('warning_pipelines')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- pipeline_health_only: `true`",
            "- read_only: `true`",
            "- pipelines_executed_by_health_check: `false`",
            "- apply_executed_by_health_check: `false`",
            "- rollback_executed_by_health_check: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- pipeline_health_json: `{run_log.get('pipeline_health_json')}`",
            f"- pipeline_health_markdown: `{run_log.get('pipeline_health_markdown')}`",
            "",
        ]
    )


def _scan_pipeline(
    *,
    definition: PipelineDefinition,
    as_of: date,
    data_root: Path,
    lookback_days: int,
    freshness_days: int,
) -> dict[str, Any]:
    stale_after_days = (
        definition.stale_after_days if definition.stale_after_days is not None else freshness_days
    )
    artifact_path, artifact_date = _latest_artifact(
        data_root=data_root,
        pattern=definition.expected_artifact_glob,
        as_of=as_of,
        lookback_days=max(lookback_days, stale_after_days + 1),
    )
    base = {
        "pipeline_id": definition.pipeline_id,
        "name": definition.name,
        "category": definition.category,
        "required": definition.required,
        "expected_artifact_glob": definition.expected_artifact_glob,
        "decision_field": definition.status_field,
        "stale_after_days": stale_after_days,
    }
    if artifact_path is None:
        status = STATUS_MISSING if definition.required else STATUS_OPTIONAL_MISSING
        reason = (
            "Required artifact missing." if definition.required else "Optional artifact missing."
        )
        return {
            **base,
            "status": status,
            "artifact_status": ARTIFACT_MISSING,
            "artifact_path": None,
            "artifact_sha256": None,
            "artifact_date": None,
            "age_days": None,
            "freshness_status": FRESHNESS_UNKNOWN,
            "decision_value": None,
            "blocking_reasons": [reason] if definition.required else [],
            "warnings": [] if definition.required else [reason],
            "notes": [],
        }

    age_days = max((as_of - artifact_date).days, 0)
    freshness_status = FRESHNESS_STALE if age_days > stale_after_days else FRESHNESS_FRESH
    payload, parse_error = _read_json_payload(artifact_path)
    decision_found = False
    decision_value: str | None = None
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    notes: list[str] = []
    safety_reasons = []

    if parse_error:
        status = STATUS_ERROR
        blocking_reasons.append(parse_error)
    else:
        decision_raw, decision_found = _field_value(payload, definition.status_field)
        decision_value = _string_value(decision_raw) if decision_found else None
        if not decision_found:
            warnings.append("Status field not found.")
        elif decision_value is None:
            warnings.append("Status field is empty.")
        safety_reasons = _artifact_safety_reasons(definition, payload)
        if safety_reasons:
            status = STATUS_CRITICAL
            blocking_reasons.extend(safety_reasons)
        else:
            status = _status_from_decision(
                definition=definition,
                decision_value=decision_value,
                decision_found=decision_found,
                freshness_status=freshness_status,
            )
            if status == STATUS_CRITICAL:
                blocking_reasons.append(f"Decision value is critical: {decision_value}.")
            elif status == STATUS_ACTION_REQUIRED:
                blocking_reasons.append(f"Decision requires manual action: {decision_value}.")
            elif status == STATUS_STALE:
                blocking_reasons.append("Required artifact is stale.")
            elif status == STATUS_WATCH:
                warnings.append(f"Decision requires watch: {decision_value}.")
            elif status == STATUS_UNKNOWN and decision_found:
                warnings.append(f"Decision value is not mapped: {decision_value}.")

    if freshness_status == FRESHNESS_STALE:
        stale_message = "Artifact is older than freshness threshold."
        if definition.required:
            if stale_message not in blocking_reasons:
                blocking_reasons.append(stale_message)
        elif stale_message not in warnings:
            warnings.append(stale_message)
    if status == STATUS_HEALTHY:
        notes.append("Artifact status is healthy.")

    return {
        **base,
        "status": status,
        "artifact_status": ARTIFACT_FOUND,
        "artifact_path": str(artifact_path),
        "artifact_sha256": _sha256(artifact_path),
        "artifact_date": artifact_date.isoformat(),
        "age_days": age_days,
        "freshness_status": freshness_status,
        "decision_value": decision_value,
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "notes": notes,
    }


def _status_from_decision(
    *,
    definition: PipelineDefinition,
    decision_value: str | None,
    decision_found: bool,
    freshness_status: str,
) -> str:
    if decision_value in definition.critical_values:
        return STATUS_CRITICAL
    if definition.required and freshness_status == FRESHNESS_STALE:
        return STATUS_STALE
    if not definition.required and freshness_status == FRESHNESS_STALE:
        return STATUS_WATCH
    if decision_value in definition.action_values:
        return STATUS_ACTION_REQUIRED
    if decision_value in definition.warning_values:
        return STATUS_WATCH
    if decision_value in definition.healthy_values:
        return STATUS_HEALTHY
    if not decision_found or decision_value is None:
        return STATUS_UNKNOWN
    return STATUS_UNKNOWN


def _artifact_safety_reasons(
    definition: PipelineDefinition,
    payload: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is True:
            reasons.append(f"{definition.pipeline_id} artifact has {field}=true.")

    if payload.get("apply_executed") is True and not definition.allow_apply_execution:
        reasons.append(f"{definition.pipeline_id} artifact unexpectedly has apply_executed=true.")
    if payload.get("rollback_executed") is True and not definition.allow_rollback_execution:
        reasons.append(
            f"{definition.pipeline_id} artifact unexpectedly has rollback_executed=true."
        )
    if payload.get("promotion_executed") is True and not definition.allow_promotion_execution:
        reasons.append(
            f"{definition.pipeline_id} artifact unexpectedly has promotion_executed=true."
        )

    production_effect = _string_value(payload.get("production_effect"))
    if production_effect and production_effect not in definition.allowed_production_effects:
        reasons.append(
            f"{definition.pipeline_id} artifact has unexpected production_effect="
            f"{production_effect}."
        )
    return reasons


def _latest_artifact(
    *,
    data_root: Path,
    pattern: str,
    as_of: date,
    lookback_days: int,
) -> tuple[Path | None, date]:
    path_pattern = _resolve_glob_pattern(data_root, pattern)
    earliest = as_of - timedelta(days=lookback_days - 1)
    candidates: list[tuple[date, float, Path]] = []
    for raw_path in glob.glob(str(path_pattern)):
        path = Path(raw_path)
        if not path.is_file():
            continue
        artifact_date = _date_from_path(path)
        if not earliest <= artifact_date <= as_of:
            continue
        try:
            modified_at = path.stat().st_mtime
        except OSError:
            modified_at = 0.0
        candidates.append((artifact_date, modified_at, path))
    if not candidates:
        return None, as_of
    artifact_date, _modified_at, path = max(candidates, key=lambda item: (item[0], item[1]))
    return path, artifact_date


def _resolve_glob_pattern(data_root: Path, pattern: str) -> Path:
    normalized = pattern.replace("\\", "/")
    raw_path = Path(pattern)
    if raw_path.is_absolute():
        return raw_path
    if normalized.startswith("data/"):
        return data_root.parent / Path(normalized)
    if normalized.startswith("outputs/"):
        return data_root.parent / Path(normalized)
    return data_root / Path(normalized)


def _date_from_path(path: Path) -> date:
    match = DATE_PATTERN.search(path.name)
    if match:
        try:
            return date(
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
            )
        except ValueError:
            pass
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()
    except OSError:
        return datetime.now(tz=UTC).date()


def _read_json_payload(path: Path) -> tuple[dict[str, Any], str]:
    if path.suffix.lower() != ".json":
        return {}, "Artifact is not JSON and cannot expose a status field."
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, f"Artifact JSON is invalid: {exc}."
    except OSError as exc:
        return {}, f"Artifact cannot be read: {exc}."
    if not isinstance(payload, dict):
        return {}, "Artifact JSON must be an object."
    return payload, ""


def _field_value(payload: dict[str, Any], field: str) -> tuple[Any, bool]:
    current: Any = payload
    for part in field.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _overall_health_status(results: list[dict[str, Any]]) -> str:
    if any(item["status"] == STATUS_CRITICAL for item in results):
        return HEALTH_CRITICAL
    if any(item["required"] is True and item["status"] == STATUS_ERROR for item in results):
        return HEALTH_ERROR
    if any(item["required"] is True and item["status"] == STATUS_MISSING for item in results):
        return HEALTH_INCOMPLETE
    if any(
        item["required"] is True and item["status"] in {STATUS_ACTION_REQUIRED, STATUS_STALE}
        for item in results
    ):
        return HEALTH_ACTION_REQUIRED
    if any(
        item["required"] is True and item["status"] in {STATUS_WATCH, STATUS_UNKNOWN}
        for item in results
    ):
        return HEALTH_WATCH
    if any(item["required"] is not True and item["status"] != STATUS_HEALTHY for item in results):
        return HEALTH_WATCH
    return HEALTH_OK


def _summary_level(health_status: str) -> str:
    return {
        HEALTH_OK: SUMMARY_NORMAL,
        HEALTH_WATCH: SUMMARY_WATCH,
        HEALTH_ACTION_REQUIRED: SUMMARY_ACTION,
        HEALTH_INCOMPLETE: SUMMARY_ACTION,
        HEALTH_CRITICAL: SUMMARY_CRITICAL,
        HEALTH_ERROR: SUMMARY_UNKNOWN,
    }.get(health_status, SUMMARY_UNKNOWN)


def _headline(health_status: str) -> str:
    return {
        HEALTH_OK: (
            "Required pipeline artifacts are available and no critical pipeline "
            "issues were detected."
        ),
        HEALTH_WATCH: (
            "Pipeline artifacts are available, but non-blocking warnings require " "monitoring."
        ),
        HEALTH_ACTION_REQUIRED: (
            "At least one required pipeline artifact is stale or requires manual action."
        ),
        HEALTH_INCOMPLETE: "At least one required pipeline artifact is missing.",
        HEALTH_CRITICAL: "A critical pipeline health issue was detected.",
        HEALTH_ERROR: "Pipeline health summary encountered an error.",
    }.get(health_status, "Pipeline health status is unknown.")


def _issue_records(
    results: list[dict[str, Any]],
    predicate: Any,
    default_reason: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in results:
        if not predicate(item):
            continue
        reasons = _strings(item.get("blocking_reasons")) or _strings(item.get("warnings"))
        records.append(
            {
                "pipeline_id": item["pipeline_id"],
                "name": item["name"],
                "status": item["status"],
                "reason": "; ".join(reasons) or default_reason,
                "artifact_path": item.get("artifact_path"),
            }
        )
    return records


def _is_warning_result(item: dict[str, Any]) -> bool:
    if item["status"] in {STATUS_WATCH, STATUS_UNKNOWN, STATUS_OPTIONAL_MISSING}:
        return True
    if item["required"] is not True and item["status"] in {
        STATUS_ACTION_REQUIRED,
        STATUS_ERROR,
        STATUS_STALE,
    }:
        return True
    return False


def _alerts(
    *,
    health_status: str,
    critical_pipelines: list[dict[str, Any]],
    warning_pipelines: list[dict[str, Any]],
) -> dict[str, list[str]]:
    critical = [
        f"{item['pipeline_id']} {item['name']}: {item['reason']}" for item in critical_pipelines
    ]
    warnings = [
        f"{item['pipeline_id']} {item['name']}: {item['reason']}" for item in warning_pipelines
    ]
    notes = ["Pipeline health summary is read-only and did not execute any pipeline."]
    if health_status == HEALTH_OK:
        notes.append("All included required pipelines are healthy.")
    return {"critical": critical, "warnings": warnings, "notes": notes}


def _operator_brief_integration(health_status: str) -> dict[str, Any]:
    adjustment = {
        HEALTH_OK: "NONE",
        HEALTH_WATCH: "WATCH",
        HEALTH_ACTION_REQUIRED: "ACTION_REQUIRED",
        HEALTH_INCOMPLETE: "ACTION_REQUIRED",
        HEALTH_CRITICAL: "CRITICAL",
        HEALTH_ERROR: "ERROR",
    }.get(health_status, "WATCH")
    return {
        "ready_for_trading_022": health_status in {HEALTH_OK, HEALTH_WATCH},
        "recommended_operator_brief_status_adjustment": adjustment,
        "notes": ["TRADING-022 can consume this pipeline health summary in a future update."],
    }


def _recommended_next_steps(health_status: str) -> list[str]:
    return {
        HEALTH_OK: [
            "Continue observation.",
            "Review stale optional pipelines if the dashboard view looks outdated.",
        ],
        HEALTH_WATCH: [
            "Review warning pipelines before relying on dashboard-only status.",
            "Confirm optional or unknown artifacts are expected for this date.",
        ],
        HEALTH_ACTION_REQUIRED: [
            "Inspect required stale or action-required pipeline artifacts.",
            "Regenerate upstream artifacts only through their own runbooks, not from TRADING-023.",
        ],
        HEALTH_INCOMPLETE: [
            "Locate or generate missing required artifacts through their owning pipelines.",
            "Do not infer system health from partial pipeline artifacts.",
        ],
        HEALTH_CRITICAL: [
            "Stop relying on the affected pipeline summaries until critical alerts are reviewed.",
            "Confirm no unexpected broker, replay, or trading execution occurred.",
        ],
        HEALTH_ERROR: [
            "Inspect the pipeline health summary run log.",
            "Fix the health summary error before using the artifact downstream.",
        ],
    }.get(health_status, ["Inspect the pipeline health summary artifact."])


def _safety_validation(critical_pipelines: list[dict[str, Any]]) -> dict[str, Any]:
    blocking = [
        item["reason"]
        for item in critical_pipelines
        if "execution" in item["reason"] or "production_effect" in item["reason"]
    ]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "read_only": True,
        "no_pipeline_execution": True,
        "no_broker_execution": True,
        "no_replay_execution": True,
        "no_trading_execution": True,
        "blocking_reasons": blocking,
    }


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    coverage = _mapping(payload.get("coverage"))
    output_artifacts = _mapping(payload.get("output_artifacts"))
    summary_json = _string_value(_mapping(output_artifacts.get("json")).get("path"))
    summary_markdown = _string_value(_mapping(output_artifacts.get("markdown")).get("path"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": _isoformat_z(generated_at),
        "run_status": "PASS" if payload.get("health_status") != HEALTH_ERROR else HEALTH_ERROR,
        "health_status": payload.get("health_status"),
        "summary_level": payload.get("summary_level"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "pipeline_health_only": True,
        "read_only": True,
        "pipelines_executed_by_health_check": False,
        "apply_executed_by_health_check": False,
        "rollback_executed_by_health_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "safe_for_scheduler": True,
        "registered_pipelines": coverage.get("registered_pipelines", 0),
        "required_pipelines": coverage.get("required_pipelines", 0),
        "available_pipelines": coverage.get("available_pipelines", 0),
        "missing_required_pipelines": coverage.get("missing_required_pipelines", 0),
        "stale_required_pipelines": coverage.get("stale_required_pipelines", 0),
        "critical_pipelines": coverage.get("critical_pipelines", 0),
        "warning_pipelines": coverage.get("warning_pipelines", 0),
        "pipeline_health_json": summary_json,
        "pipeline_health_markdown": summary_markdown,
    }


def _error_payload(
    *,
    as_of: date,
    data_root: Path,
    lookback_days: int,
    freshness_days: int,
    include_optional_pipelines: bool,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    _ = data_root
    alerts = {"critical": [error], "warnings": [], "notes": []}
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": _isoformat_z(generated_at),
        "lookback_days": lookback_days,
        "freshness_days": freshness_days,
        "include_optional_pipelines": include_optional_pipelines,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "pipeline_health_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "pipelines_executed_by_health_check": False,
        "apply_executed_by_health_check": False,
        "rollback_executed_by_health_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "health_status": HEALTH_ERROR,
        "summary_level": SUMMARY_UNKNOWN,
        "headline": _headline(HEALTH_ERROR),
        "coverage": {
            "registered_pipelines": 0,
            "required_pipelines": 0,
            "available_pipelines": 0,
            "missing_required_pipelines": 0,
            "stale_required_pipelines": 0,
            "critical_pipelines": 0,
            "warning_pipelines": 0,
        },
        "pipeline_results": [],
        "missing_required_pipelines": [],
        "stale_pipelines": [],
        "critical_pipelines": [],
        "warning_pipelines": [],
        "operator_brief_integration": _operator_brief_integration(HEALTH_ERROR),
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(HEALTH_ERROR),
        "safety_validation": {
            "status": "FAIL",
            "read_only": True,
            "no_pipeline_execution": True,
            "no_broker_execution": True,
            "no_replay_execution": True,
            "no_trading_execution": True,
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
            "created_by": "scripts/run_pipeline_health_summary.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_pipeline_health_artifacts": True,
        },
    }


def _pipeline_table_rows(results: list[dict[str, Any]]) -> list[str]:
    if not results:
        return ["| None | - | - | - | - |"]
    rows = []
    for item in results:
        artifact = item.get("artifact_path") or "-"
        decision = item.get("decision_value") or "-"
        freshness = item.get("freshness_status") or "-"
        rows.append(
            "| "
            f"{item.get('pipeline_id')} | "
            f"`{item.get('status')}` | "
            f"`{freshness}` | "
            f"`{decision}` | "
            f"`{artifact}` |"
        )
    return rows


def _markdown_bullets(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _isoformat_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _mappings(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if item is not None and str(item)]
    text = str(value)
    return [text] if text else []


def _string_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _assert_pipeline_health_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("pipeline health summary production_effect must remain none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("pipeline health summary must remain manual_review_only")
    if payload.get("pipeline_health_only") is not True:
        raise ValueError("pipeline health summary must remain pipeline_health_only")
    if payload.get("read_only") is not True:
        raise ValueError("pipeline health summary must remain read_only")
    if payload.get("pipelines_executed_by_health_check") is not False:
        raise ValueError("pipeline health summary must not execute pipelines")
    if payload.get("apply_executed_by_health_check") is not False:
        raise ValueError("pipeline health summary must not execute apply")
    if payload.get("rollback_executed_by_health_check") is not False:
        raise ValueError("pipeline health summary must not execute rollback")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"pipeline health summary must keep {field}=false")
    if payload.get("safe_for_scheduler") is not True:
        raise ValueError("pipeline health summary should be scheduler-safe")
