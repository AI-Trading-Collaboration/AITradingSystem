from __future__ import annotations

import glob
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "data_freshness_summary"
RUN_REPORT_TYPE = "data_freshness_summary_run"
TASK_ID = "TRADING-024"
MODE = "data_freshness_summary_only"
PRODUCTION_EFFECT_NONE = "none"
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_FRESHNESS_DAYS = 2

FRESHNESS_OK = "OK"
FRESHNESS_WATCH = "WATCH"
FRESHNESS_STALE = "STALE"
FRESHNESS_MISSING = "MISSING"
FRESHNESS_CRITICAL = "CRITICAL"
FRESHNESS_ERROR = "ERROR"

STATUS_FRESH = "FRESH"
STATUS_WATCH = "WATCH"
STATUS_STALE = "STALE"
STATUS_MISSING = "MISSING"
STATUS_OPTIONAL_MISSING = "OPTIONAL_MISSING"
STATUS_CRITICAL = "CRITICAL"
STATUS_UNKNOWN = "UNKNOWN"
STATUS_ERROR = "ERROR"

ARTIFACT_FOUND = "FOUND"
ARTIFACT_MISSING = "MISSING"
DATA_FRESH = "FRESH"
DATA_STALE = "STALE"
DATA_UNKNOWN = "UNKNOWN"

SUMMARY_NORMAL = "NORMAL"
SUMMARY_WATCH = "WATCH"
SUMMARY_ACTION = "ACTION"
SUMMARY_CRITICAL = "CRITICAL"
SUMMARY_UNKNOWN = "UNKNOWN"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"
DATE_PATTERN = re.compile(r"(?P<year>\d{4})[-_](?P<month>\d{2})[-_](?P<day>\d{2})")

READ_ONLY_SOURCE_IDS = {
    "parameter_governance_digest",
    "pipeline_health_summary",
    "daily_operator_brief",
    "parameter_governance_summary",
    "shadow_vs_production_review",
    "lifecycle_audit",
}

FRESHNESS_CONTRACT: dict[str, Any] = {
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
    "runs_data_freshness_summary_script": False,
    "runs_market_pipeline": False,
    "runs_backtest_pipeline": False,
    "runs_scoring_pipeline": False,
    "runs_data_download": False,
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
    "data_freshness_only": True,
    "read_only": True,
}


@dataclass(frozen=True)
class DataSourceDefinition:
    source_id: str
    name: str
    category: str
    required: bool
    expected_artifact_glob: str
    date_fields: tuple[str, ...] = ("date", "data_date", "as_of_date", "report_date")
    status_field: str | None = None
    healthy_values: tuple[str, ...] = ("OK", "PASS", "AVAILABLE")
    action_values: tuple[str, ...] = (
        "ACTION_REQUIRED",
        "INSUFFICIENT_DATA",
        "INPUT_MISSING",
        "INPUT_INVALID",
    )
    critical_values: tuple[str, ...] = (
        "URGENT",
        "ERROR",
        "SAFETY_BLOCKED",
        "SAFETY_ANOMALY",
        "CRITICAL",
    )
    warning_values: tuple[str, ...] = ("WATCH", "WARNING", "PASS_WITH_WARNINGS")
    stale_after_days: int | None = None
    allow_apply_execution: bool = False
    allow_rollback_execution: bool = False
    allow_promotion_execution: bool = False
    allowed_production_effects: tuple[str, ...] = (PRODUCTION_EFFECT_NONE,)
    allow_modified_time_date: bool = True


DEFAULT_DATA_SOURCE_REGISTRY: tuple[DataSourceDefinition, ...] = (
    DataSourceDefinition(
        source_id="parameter_governance_digest",
        name="Parameter Governance Daily Digest",
        category="governance",
        required=True,
        expected_artifact_glob=(
            "data/derived/weight_iterations/governance/digests/"
            "parameter_governance_daily_digest_*.json"
        ),
        date_fields=("date", "generated_for_date"),
        status_field="digest_status",
        healthy_values=("OK",),
        warning_values=("WATCH",),
        action_values=("ACTION_REQUIRED", "INPUT_MISSING", "INPUT_INVALID"),
        critical_values=("URGENT", "SAFETY_BLOCKED", "ERROR"),
    ),
    DataSourceDefinition(
        source_id="pipeline_health_summary",
        name="Pipeline Health Summary",
        category="operations",
        required=True,
        expected_artifact_glob="data/derived/pipeline_health/pipeline_health_summary_*.json",
        date_fields=("date", "generated_for_date"),
        status_field="health_status",
        healthy_values=("OK",),
        warning_values=("WATCH",),
        action_values=("ACTION_REQUIRED", "INCOMPLETE"),
        critical_values=("CRITICAL", "ERROR"),
    ),
    DataSourceDefinition(
        source_id="daily_operator_brief",
        name="Daily Trading System Operator Brief",
        category="operations",
        required=True,
        expected_artifact_glob=(
            "data/derived/operator_briefs/daily_trading_system_operator_brief_*.json"
        ),
        date_fields=("date", "generated_for_date"),
        status_field="brief_status",
        healthy_values=("OK",),
        warning_values=("WATCH",),
        action_values=("ACTION_REQUIRED", "INPUT_MISSING", "INPUT_INVALID"),
        critical_values=("URGENT", "SAFETY_BLOCKED", "ERROR"),
    ),
    DataSourceDefinition(
        source_id="parameter_governance_summary",
        name="Parameter Governance Summary",
        category="governance",
        required=False,
        expected_artifact_glob=(
            "data/derived/weight_iterations/governance/parameter_governance_summary_*.json"
        ),
        date_fields=("date", "generated_for_date"),
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
    DataSourceDefinition(
        source_id="shadow_weights",
        name="Current Shadow Weights",
        category="weights",
        required=False,
        expected_artifact_glob="data/derived/weight_iterations/shadow/current_shadow_weights.json",
        date_fields=("date", "as_of", "as_of_date", "updated_for_date"),
        status_field="status",
        healthy_values=("ACTIVE", "OK", "AVAILABLE"),
        action_values=("INSUFFICIENT_DATA", "MISSING"),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
    ),
    DataSourceDefinition(
        source_id="shadow_vs_production_review",
        name="Shadow vs Production Multi-day Review",
        category="weight_iteration",
        required=False,
        expected_artifact_glob=(
            "data/derived/weight_iterations/comparison/reviews/"
            "shadow_vs_production_review_*.json"
        ),
        date_fields=("date", "review_date", "generated_for_date"),
        status_field="review_decision",
        healthy_values=(
            "CONTINUE_OBSERVATION",
            "SHADOW_LOOKS_BETTER",
            "SHADOW_LOOKS_WORSE",
        ),
        action_values=("INSUFFICIENT_HISTORY", "INSUFFICIENT_DATA"),
        critical_values=("SAFETY_BLOCKED", "ERROR"),
    ),
    DataSourceDefinition(
        source_id="lifecycle_audit",
        name="Shadow Promotion Lifecycle Audit",
        category="governance",
        required=False,
        expected_artifact_glob=(
            "data/derived/weight_iterations/promotion/audit/"
            "shadow_promotion_lifecycle_audit_*.json"
        ),
        date_fields=("date", "audit_date", "promotion_date"),
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
    DataSourceDefinition(
        source_id="market_report",
        name="Market Report",
        category="market",
        required=False,
        expected_artifact_glob="outputs/reports/market_report_*.json",
        date_fields=("date", "as_of", "as_of_date", "report_date"),
        status_field="status",
        healthy_values=("OK", "PASS", "AVAILABLE"),
        action_values=("FAIL", "BLOCKED", "INSUFFICIENT_DATA"),
        critical_values=("CRITICAL", "ERROR", "SAFETY_BLOCKED"),
    ),
    DataSourceDefinition(
        source_id="backtest_summary",
        name="Backtest Summary",
        category="backtest",
        required=False,
        expected_artifact_glob="outputs/backtests/backtest_summary_*.json",
        date_fields=("date", "as_of", "as_of_date", "report_date"),
        status_field="status",
        healthy_values=("OK", "PASS", "AVAILABLE"),
        action_values=("FAIL", "BLOCKED", "INSUFFICIENT_DATA"),
        critical_values=("CRITICAL", "ERROR", "SAFETY_BLOCKED"),
        stale_after_days=7,
    ),
    DataSourceDefinition(
        source_id="price_data_cache",
        name="Price Data Cache",
        category="market_data",
        required=False,
        expected_artifact_glob="data/raw/prices_daily.csv",
        date_fields=("date", "as_of_date", "data_date"),
        status_field=None,
    ),
    DataSourceDefinition(
        source_id="news_or_signal_cache",
        name="News or Signal Cache",
        category="signals",
        required=False,
        expected_artifact_glob="data/raw/news_or_signal_cache_*.json",
        date_fields=("date", "as_of_date", "data_date", "captured_at"),
        status_field="status",
        healthy_values=("OK", "PASS", "AVAILABLE"),
        action_values=("STALE", "INSUFFICIENT_DATA"),
        critical_values=("CRITICAL", "ERROR", "SAFETY_BLOCKED"),
    ),
)


def default_data_freshness_root(data_root: Path) -> Path:
    return data_root / "derived" / "data_freshness"


def default_data_freshness_json_path(data_root: Path, as_of: date) -> Path:
    return default_data_freshness_root(data_root) / (
        f"data_freshness_summary_{as_of.isoformat()}.json"
    )


def default_data_freshness_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_data_freshness_root(data_root)
        / "logs"
        / f"data_freshness_summary_run_{as_of.isoformat()}.json"
    )


def write_data_freshness_summary(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    market_date: date | None = None,
    fail_on_critical: bool = False,
    include_optional_sources: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
    registry: tuple[DataSourceDefinition, ...] = DEFAULT_DATA_SOURCE_REGISTRY,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_data_freshness_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_data_freshness_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_data_freshness_summary_payload(
            as_of=as_of,
            data_root=data_root,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
            market_date=market_date,
            include_optional_sources=include_optional_sources,
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
            market_date=market_date,
            include_optional_sources=include_optional_sources,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    write_json_atomic(output_json_path, payload, sort_keys=False)
    write_text_atomic(output_md_path, render_data_freshness_summary_markdown(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    write_json_atomic(run_log_json_path, run_log, sort_keys=False)
    write_text_atomic(run_log_md_path, render_data_freshness_summary_run_log(run_log))

    if fail_on_critical and payload.get("freshness_status") == FRESHNESS_CRITICAL:
        raise SystemExit(2)
    return payload


def build_data_freshness_summary_payload(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    market_date: date | None = None,
    include_optional_sources: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
    registry: tuple[DataSourceDefinition, ...] = DEFAULT_DATA_SOURCE_REGISTRY,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if freshness_days < 0:
        raise ValueError("freshness_days must be non-negative")

    generated = generated_at or datetime.now(tz=UTC)
    freshness_reference_date = market_date or as_of
    output_json_path = output_json_path or default_data_freshness_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_data_freshness_run_log_json_path(
        data_root, as_of
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")
    included_registry = tuple(
        definition for definition in registry if definition.required or include_optional_sources
    )

    source_results = [
        _scan_source(
            definition=definition,
            as_of=as_of,
            freshness_reference_date=freshness_reference_date,
            data_root=data_root,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
        )
        for definition in included_registry
    ]
    missing_required = _issue_records(
        source_results,
        lambda item: item["required"] is True and item["status"] == STATUS_MISSING,
        "Required source artifact is missing.",
    )
    stale_sources = _issue_records(
        source_results,
        lambda item: item["freshness_status"] == DATA_STALE,
        "Source data is older than freshness threshold.",
    )
    critical_sources = _issue_records(
        source_results,
        lambda item: item["status"] == STATUS_CRITICAL,
        "Critical source freshness condition detected.",
    )
    warning_sources = _issue_records(
        source_results,
        _is_warning_result,
        "Source requires attention or has a non-blocking warning.",
    )
    freshness_status = _overall_freshness_status(source_results)
    alerts = _alerts(
        freshness_status=freshness_status,
        critical_sources=critical_sources,
        warning_sources=warning_sources,
    )
    safety_validation = _safety_validation(critical_sources)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": _isoformat_z(generated),
        "lookback_days": lookback_days,
        "freshness_days": freshness_days,
        "market_date": None if market_date is None else market_date.isoformat(),
        "freshness_reference_date": freshness_reference_date.isoformat(),
        "include_optional_sources": include_optional_sources,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "data_freshness_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "data_downloaded_by_freshness_check": False,
        "pipelines_executed_by_freshness_check": False,
        "apply_executed_by_freshness_check": False,
        "rollback_executed_by_freshness_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "freshness_status": freshness_status,
        "summary_level": _summary_level(freshness_status),
        "headline": _headline(freshness_status),
        "coverage": {
            "registered_sources": len(source_results),
            "required_sources": sum(1 for item in source_results if item["required"]),
            "available_sources": sum(
                1 for item in source_results if item["artifact_status"] == ARTIFACT_FOUND
            ),
            "missing_required_sources": len(missing_required),
            "stale_required_sources": sum(
                1
                for item in source_results
                if item["required"] is True and item["freshness_status"] == DATA_STALE
            ),
            "critical_sources": len(critical_sources),
            "warning_sources": len(warning_sources),
        },
        "source_results": source_results,
        "missing_required_sources": missing_required,
        "stale_sources": stale_sources,
        "critical_sources": critical_sources,
        "warning_sources": warning_sources,
        "operator_brief_integration": _operator_brief_integration(freshness_status),
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(freshness_status),
        "safety_validation": safety_validation,
        "output_artifacts": {
            "json": {"path": str(output_json_path)},
            "markdown": {"path": str(output_md_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
        "freshness_contract": dict(FRESHNESS_CONTRACT),
        "audit": {
            "created_by": "scripts/run_data_freshness_summary.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_data_freshness_artifacts": True,
            "registry_total_sources": len(registry),
        },
    }
    _assert_data_freshness_safety_invariants(payload)
    return payload


def render_data_freshness_summary_markdown(payload: dict[str, Any]) -> str:
    freshness_status = _string_value(payload.get("freshness_status")) or FRESHNESS_ERROR
    coverage = _mapping(payload.get("coverage"))
    required = [
        item for item in _mappings(payload.get("source_results")) if item.get("required") is True
    ]
    optional = [
        item
        for item in _mappings(payload.get("source_results"))
        if item.get("required") is not True
    ]
    alerts = _mapping(payload.get("alerts"))

    lines = [f"# Data Freshness Summary - {payload.get('date')}", ""]
    if freshness_status == FRESHNESS_CRITICAL:
        lines.extend(["## CRITICAL: Data Freshness Issue Detected", ""])
    elif freshness_status == FRESHNESS_STALE:
        lines.extend(["## Stale Required Data Detected", ""])
    elif freshness_status == FRESHNESS_MISSING:
        lines.extend(["## Required Data Missing", ""])

    lines.extend(
        [
            "## 1. Freshness Summary",
            "",
            f"- Freshness Status: `{freshness_status}`",
            f"- Summary Level: `{payload.get('summary_level', SUMMARY_UNKNOWN)}`",
            f"- Headline: {payload.get('headline') or ''}",
            f"- Registered Sources: `{coverage.get('registered_sources', 0)}`",
            f"- Required Sources: `{coverage.get('required_sources', 0)}`",
            "- Missing Required Sources: " f"`{coverage.get('missing_required_sources', 0)}`",
            f"- Stale Required Sources: `{coverage.get('stale_required_sources', 0)}`",
            f"- Critical Sources: `{coverage.get('critical_sources', 0)}`",
            f"- Warning Sources: `{coverage.get('warning_sources', 0)}`",
            "",
            "## 2. Required Sources",
            "",
            "| Source | Status | Freshness | Data Date | Age Days | Decision | Artifact |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    lines.extend(_source_table_rows(required))
    lines.extend(
        [
            "",
            "## 3. Optional Sources",
            "",
            "| Source | Status | Freshness | Data Date | Age Days | Decision | Artifact |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    if optional:
        lines.extend(_source_table_rows(optional))
    else:
        lines.append("| None | - | - | - | - | - | - |")

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
                "This task is read-only and did not download data, execute pipelines, "
                "broker, replay, or trading processes."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def render_data_freshness_summary_run_log(run_log: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Data Freshness Summary Run Log - {run_log.get('date')}",
            "",
            f"- Run Status: `{run_log.get('run_status')}`",
            f"- Freshness Status: `{run_log.get('freshness_status')}`",
            f"- Summary Level: `{run_log.get('summary_level')}`",
            f"- Registered Sources: `{run_log.get('registered_sources')}`",
            f"- Missing Required Sources: `{run_log.get('missing_required_sources')}`",
            f"- Stale Required Sources: `{run_log.get('stale_required_sources')}`",
            f"- Critical Sources: `{run_log.get('critical_sources')}`",
            f"- Warning Sources: `{run_log.get('warning_sources')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- data_freshness_only: `true`",
            "- read_only: `true`",
            "- data_downloaded_by_freshness_check: `false`",
            "- pipelines_executed_by_freshness_check: `false`",
            "- apply_executed_by_freshness_check: `false`",
            "- rollback_executed_by_freshness_check: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- data_freshness_json: `{run_log.get('data_freshness_json')}`",
            f"- data_freshness_markdown: `{run_log.get('data_freshness_markdown')}`",
            "",
        ]
    )


def _scan_source(
    *,
    definition: DataSourceDefinition,
    as_of: date,
    freshness_reference_date: date,
    data_root: Path,
    lookback_days: int,
    freshness_days: int,
) -> dict[str, Any]:
    stale_after_days = (
        definition.stale_after_days if definition.stale_after_days is not None else freshness_days
    )
    artifact_path, artifact_date, artifact_date_source = _latest_artifact(
        data_root=data_root,
        pattern=definition.expected_artifact_glob,
        as_of=as_of,
        lookback_days=max(lookback_days, stale_after_days + 1),
    )
    base = {
        "source_id": definition.source_id,
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
            "Required source artifact was not found."
            if definition.required
            else "Optional source artifact was not found."
        )
        return {
            **base,
            "status": status,
            "artifact_status": ARTIFACT_MISSING,
            "artifact_path": None,
            "artifact_sha256": None,
            "artifact_date": None,
            "data_date": None,
            "date_source": None,
            "age_days": None,
            "freshness_status": DATA_UNKNOWN,
            "decision_value": None,
            "blocking_reasons": [reason] if definition.required else [],
            "warnings": [] if definition.required else [reason],
            "notes": [],
        }

    payload, parse_error = _read_json_payload_if_needed(artifact_path, definition)
    data_date, date_source = _data_date(
        definition=definition,
        payload=payload,
        path=artifact_path,
        artifact_date=artifact_date,
        artifact_date_source=artifact_date_source,
    )
    if data_date is None:
        age_days = None
        freshness_status = DATA_UNKNOWN
    else:
        age_days = max((freshness_reference_date - data_date).days, 0)
        freshness_status = DATA_STALE if age_days > stale_after_days else DATA_FRESH

    decision_found = False
    decision_value: str | None = None
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    notes: list[str] = []
    safety_reasons: list[str] = []

    if parse_error:
        status = STATUS_ERROR
        blocking_reasons.append(parse_error)
    else:
        if definition.status_field is not None:
            decision_raw, decision_found = _field_value(payload, definition.status_field)
            decision_value = _string_value(decision_raw) if decision_found else None
            if not decision_found:
                warnings.append("Status field not found.")
            elif decision_value is None:
                warnings.append("Status field is empty.")
        else:
            decision_found = True
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
            elif status == STATUS_STALE and decision_value in definition.action_values:
                blocking_reasons.append(f"Decision requires manual action: {decision_value}.")
            elif status == STATUS_STALE:
                blocking_reasons.append("Required source artifact is stale.")
            elif status == STATUS_WATCH and decision_value in definition.warning_values:
                warnings.append(f"Decision requires watch: {decision_value}.")
            elif status == STATUS_UNKNOWN and definition.status_field is not None:
                if decision_found and decision_value is not None:
                    warnings.append(f"Decision value is not mapped: {decision_value}.")

    if freshness_status == DATA_STALE:
        stale_message = "Source data is older than freshness threshold."
        if definition.required:
            if stale_message not in blocking_reasons:
                blocking_reasons.append(stale_message)
        elif stale_message not in warnings:
            warnings.append(stale_message)
    elif freshness_status == DATA_UNKNOWN:
        unknown_message = "Unable to derive data date."
        if unknown_message not in warnings:
            warnings.append(unknown_message)
    if status == STATUS_FRESH:
        notes.append("Source artifact is fresh.")

    return {
        **base,
        "status": status,
        "artifact_status": ARTIFACT_FOUND,
        "artifact_path": str(artifact_path),
        "artifact_sha256": _sha256(artifact_path),
        "artifact_date": artifact_date.isoformat(),
        "data_date": None if data_date is None else data_date.isoformat(),
        "date_source": date_source,
        "age_days": age_days,
        "freshness_status": freshness_status,
        "decision_value": decision_value,
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "notes": notes,
    }


def _status_from_decision(
    *,
    definition: DataSourceDefinition,
    decision_value: str | None,
    decision_found: bool,
    freshness_status: str,
) -> str:
    if decision_value in definition.critical_values:
        return STATUS_CRITICAL
    if definition.required and freshness_status == DATA_STALE:
        return STATUS_STALE
    if not definition.required and freshness_status == DATA_STALE:
        return STATUS_WATCH
    if freshness_status == DATA_UNKNOWN:
        return STATUS_UNKNOWN
    if decision_value in definition.action_values:
        return STATUS_STALE
    if decision_value in definition.warning_values:
        return STATUS_WATCH
    if decision_value in definition.healthy_values or definition.status_field is None:
        return STATUS_FRESH
    if not decision_found or decision_value is None:
        return STATUS_UNKNOWN
    return STATUS_UNKNOWN


def _artifact_safety_reasons(
    definition: DataSourceDefinition,
    payload: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    critical_true_fields = (
        "broker_execution",
        "replay_execution",
        "trading_execution",
        "data_downloaded_by_freshness_check",
        "pipelines_executed_by_freshness_check",
    )
    for field in critical_true_fields:
        if _deep_truthy_field(payload, field):
            reasons.append(f"{definition.source_id} artifact has {field}=true.")

    if payload.get("apply_executed") is True and not definition.allow_apply_execution:
        reasons.append(f"{definition.source_id} artifact unexpectedly has apply_executed=true.")
    if payload.get("rollback_executed") is True and not definition.allow_rollback_execution:
        reasons.append(f"{definition.source_id} artifact unexpectedly has rollback_executed=true.")
    if payload.get("promotion_executed") is True and not definition.allow_promotion_execution:
        reasons.append(f"{definition.source_id} artifact unexpectedly has promotion_executed=true.")

    production_effect = _string_value(payload.get("production_effect"))
    if production_effect and production_effect not in definition.allowed_production_effects:
        reasons.append(
            f"{definition.source_id} artifact has unexpected production_effect="
            f"{production_effect}."
        )
    return reasons


def _latest_artifact(
    *,
    data_root: Path,
    pattern: str,
    as_of: date,
    lookback_days: int,
) -> tuple[Path | None, date, str]:
    path_pattern = _resolve_glob_pattern(data_root, pattern)
    earliest = as_of - timedelta(days=lookback_days - 1)
    candidates: list[tuple[date, float, Path, str]] = []
    for raw_path in glob.glob(str(path_pattern)):
        path = Path(raw_path)
        if not path.is_file():
            continue
        artifact_date, date_source = _artifact_date_for_sorting(path)
        if not earliest <= artifact_date <= as_of:
            continue
        try:
            modified_at = path.stat().st_mtime
        except OSError:
            modified_at = 0.0
        candidates.append((artifact_date, modified_at, path, date_source))
    if not candidates:
        return None, as_of, "none"
    artifact_date, _modified_at, path, date_source = max(
        candidates, key=lambda item: (item[0], item[1])
    )
    return path, artifact_date, date_source


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


def _artifact_date_for_sorting(path: Path) -> tuple[date, str]:
    filename_date = _date_from_filename(path)
    if filename_date is not None:
        return filename_date, "filename"
    return _modified_date(path), "modified_time"


def _data_date(
    *,
    definition: DataSourceDefinition,
    payload: dict[str, Any],
    path: Path,
    artifact_date: date,
    artifact_date_source: str,
) -> tuple[date | None, str | None]:
    if payload:
        for field in definition.date_fields:
            raw_value, found = _field_value(payload, field)
            parsed = _parse_date_value(raw_value) if found else None
            if parsed is not None:
                return parsed, f"json_field:{field}"
    filename_date = _date_from_filename(path)
    if filename_date is not None:
        return filename_date, "filename"
    if definition.allow_modified_time_date and artifact_date_source == "modified_time":
        return artifact_date, "modified_time"
    if definition.allow_modified_time_date:
        return _modified_date(path), "modified_time"
    return None, None


def _date_from_filename(path: Path) -> date | None:
    match = DATE_PATTERN.search(path.name)
    if not match:
        return None
    try:
        return date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
    except ValueError:
        return None


def _modified_date(path: Path) -> date:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()
    except OSError:
        return datetime.now(tz=UTC).date()


def _parse_date_value(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    filename_match = DATE_PATTERN.search(text)
    if filename_match:
        try:
            return date(
                int(filename_match.group("year")),
                int(filename_match.group("month")),
                int(filename_match.group("day")),
            )
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _read_json_payload_if_needed(
    path: Path,
    definition: DataSourceDefinition,
) -> tuple[dict[str, Any], str]:
    if path.suffix.lower() != ".json":
        return {}, ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, f"Artifact JSON is invalid: {exc}."
    except OSError as exc:
        return {}, f"Artifact cannot be read: {exc}."
    if not isinstance(payload, dict):
        return {}, "Artifact JSON must be an object."
    _ = definition
    return payload, ""


def _field_value(payload: dict[str, Any], field: str | None) -> tuple[Any, bool]:
    if not field:
        return None, False
    current: Any = payload
    for part in field.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _deep_truthy_field(value: Any, field: str) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == field and child is True:
                return True
            if _deep_truthy_field(child, field):
                return True
    elif isinstance(value, list):
        return any(_deep_truthy_field(item, field) for item in value)
    return False


def _overall_freshness_status(results: list[dict[str, Any]]) -> str:
    if any(item["status"] == STATUS_CRITICAL for item in results):
        return FRESHNESS_CRITICAL
    if any(item["required"] is True and item["status"] == STATUS_ERROR for item in results):
        return FRESHNESS_ERROR
    if any(item["required"] is True and item["status"] == STATUS_MISSING for item in results):
        return FRESHNESS_MISSING
    if any(item["required"] is True and item["status"] == STATUS_STALE for item in results):
        return FRESHNESS_STALE
    if any(
        item["required"] is True and item["status"] in {STATUS_WATCH, STATUS_UNKNOWN}
        for item in results
    ):
        return FRESHNESS_WATCH
    if any(item["required"] is not True and item["status"] != STATUS_FRESH for item in results):
        return FRESHNESS_WATCH
    return FRESHNESS_OK


def _summary_level(freshness_status: str) -> str:
    return {
        FRESHNESS_OK: SUMMARY_NORMAL,
        FRESHNESS_WATCH: SUMMARY_WATCH,
        FRESHNESS_STALE: SUMMARY_ACTION,
        FRESHNESS_MISSING: SUMMARY_ACTION,
        FRESHNESS_CRITICAL: SUMMARY_CRITICAL,
        FRESHNESS_ERROR: SUMMARY_UNKNOWN,
    }.get(freshness_status, SUMMARY_UNKNOWN)


def _headline(freshness_status: str) -> str:
    return {
        FRESHNESS_OK: "Required data sources are fresh enough for today's system outputs.",
        FRESHNESS_WATCH: (
            "Required data sources are usable, but freshness warnings require monitoring."
        ),
        FRESHNESS_STALE: "At least one required data source is stale.",
        FRESHNESS_MISSING: "At least one required data source is missing.",
        FRESHNESS_CRITICAL: "A critical data freshness safety issue was detected.",
        FRESHNESS_ERROR: "Data freshness summary encountered an error.",
    }.get(freshness_status, "Data freshness status is unknown.")


def _issue_records(
    results: list[dict[str, Any]],
    predicate: Callable[[dict[str, Any]], bool],
    default_reason: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in results:
        if not predicate(item):
            continue
        reasons = _strings(item.get("blocking_reasons")) or _strings(item.get("warnings"))
        records.append(
            {
                "source_id": item["source_id"],
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
        STATUS_ERROR,
        STATUS_STALE,
    }:
        return True
    return False


def _alerts(
    *,
    freshness_status: str,
    critical_sources: list[dict[str, Any]],
    warning_sources: list[dict[str, Any]],
) -> dict[str, list[str]]:
    critical = [
        f"{item['source_id']} {item['name']}: {item['reason']}" for item in critical_sources
    ]
    warnings = [f"{item['source_id']} {item['name']}: {item['reason']}" for item in warning_sources]
    notes = ["Data freshness summary is read-only and did not download or refresh data."]
    if freshness_status == FRESHNESS_OK:
        notes.append("All included required data sources are fresh.")
    return {"critical": critical, "warnings": warnings, "notes": notes}


def _operator_brief_integration(freshness_status: str) -> dict[str, Any]:
    adjustment = {
        FRESHNESS_OK: "NONE",
        FRESHNESS_WATCH: "WATCH",
        FRESHNESS_STALE: "ACTION_REQUIRED",
        FRESHNESS_MISSING: "ACTION_REQUIRED",
        FRESHNESS_CRITICAL: "CRITICAL",
        FRESHNESS_ERROR: "ERROR",
    }.get(freshness_status, "WATCH")
    return {
        "ready_for_trading_022": freshness_status in {FRESHNESS_OK, FRESHNESS_WATCH},
        "recommended_operator_brief_status_adjustment": adjustment,
        "notes": ["TRADING-022 can consume this data freshness summary in a future update."],
    }


def _recommended_next_steps(freshness_status: str) -> list[str]:
    return {
        FRESHNESS_OK: [
            "Continue observation.",
            "Review optional missing sources if market-facing reports require them.",
        ],
        FRESHNESS_WATCH: [
            "Review warning sources before relying on dashboard-only status.",
            "Confirm optional or unknown artifacts are expected for this date.",
        ],
        FRESHNESS_STALE: [
            "Inspect required stale source artifacts.",
            "Regenerate upstream data only through its owning runbook, not from TRADING-024.",
        ],
        FRESHNESS_MISSING: [
            "Locate or generate missing required artifacts through their owning pipelines.",
            "Do not infer data freshness from partial artifacts.",
        ],
        FRESHNESS_CRITICAL: [
            "Stop relying on affected freshness conclusions until critical alerts are reviewed.",
            (
                "Confirm no unexpected data download, pipeline, broker, replay, or trading "
                "execution occurred."
            ),
        ],
        FRESHNESS_ERROR: [
            "Inspect the data freshness summary run log.",
            "Fix the freshness summary error before using the artifact downstream.",
        ],
    }.get(freshness_status, ["Inspect the data freshness summary artifact."])


def _safety_validation(critical_sources: list[dict[str, Any]]) -> dict[str, Any]:
    blocking = [
        item["reason"]
        for item in critical_sources
        if "execution" in item["reason"]
        or "production_effect" in item["reason"]
        or "downloaded_by_freshness_check" in item["reason"]
        or "pipelines_executed_by_freshness_check" in item["reason"]
    ]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "read_only": True,
        "no_data_download": True,
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
        "run_status": (
            "PASS" if payload.get("freshness_status") != FRESHNESS_ERROR else FRESHNESS_ERROR
        ),
        "freshness_status": payload.get("freshness_status"),
        "summary_level": payload.get("summary_level"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "data_freshness_only": True,
        "read_only": True,
        "data_downloaded_by_freshness_check": False,
        "pipelines_executed_by_freshness_check": False,
        "apply_executed_by_freshness_check": False,
        "rollback_executed_by_freshness_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "safe_for_scheduler": True,
        "registered_sources": coverage.get("registered_sources", 0),
        "required_sources": coverage.get("required_sources", 0),
        "available_sources": coverage.get("available_sources", 0),
        "missing_required_sources": coverage.get("missing_required_sources", 0),
        "stale_required_sources": coverage.get("stale_required_sources", 0),
        "critical_sources": coverage.get("critical_sources", 0),
        "warning_sources": coverage.get("warning_sources", 0),
        "data_freshness_json": summary_json,
        "data_freshness_markdown": summary_markdown,
    }


def _error_payload(
    *,
    as_of: date,
    data_root: Path,
    lookback_days: int,
    freshness_days: int,
    market_date: date | None,
    include_optional_sources: bool,
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
        "market_date": None if market_date is None else market_date.isoformat(),
        "freshness_reference_date": (market_date or as_of).isoformat(),
        "include_optional_sources": include_optional_sources,
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "data_freshness_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "data_downloaded_by_freshness_check": False,
        "pipelines_executed_by_freshness_check": False,
        "apply_executed_by_freshness_check": False,
        "rollback_executed_by_freshness_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "freshness_status": FRESHNESS_ERROR,
        "summary_level": SUMMARY_UNKNOWN,
        "headline": _headline(FRESHNESS_ERROR),
        "coverage": {
            "registered_sources": 0,
            "required_sources": 0,
            "available_sources": 0,
            "missing_required_sources": 0,
            "stale_required_sources": 0,
            "critical_sources": 0,
            "warning_sources": 0,
        },
        "source_results": [],
        "missing_required_sources": [],
        "stale_sources": [],
        "critical_sources": [],
        "warning_sources": [],
        "operator_brief_integration": _operator_brief_integration(FRESHNESS_ERROR),
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(FRESHNESS_ERROR),
        "safety_validation": {
            "status": "FAIL",
            "read_only": True,
            "no_data_download": True,
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
        "freshness_contract": dict(FRESHNESS_CONTRACT),
        "audit": {
            "created_by": "scripts/run_data_freshness_summary.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_data_freshness_artifacts": True,
        },
    }


def _source_table_rows(results: list[dict[str, Any]]) -> list[str]:
    if not results:
        return ["| None | - | - | - | - | - | - |"]
    rows = []
    for item in results:
        artifact = item.get("artifact_path") or "-"
        decision = item.get("decision_value") or "-"
        freshness = item.get("freshness_status") or "-"
        data_date = item.get("data_date") or "-"
        age_days = item.get("age_days")
        rows.append(
            "| "
            f"{item.get('source_id')} | "
            f"`{item.get('status')}` | "
            f"`{freshness}` | "
            f"`{data_date}` | "
            f"`{'-' if age_days is None else age_days}` | "
            f"`{decision}` | "
            f"`{artifact}` |"
        )
    return rows


def _markdown_bullets(values: list[str]) -> list[str]:
    if not values:
        return ["- None."]
    return [f"- {value}" for value in values]


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


def _assert_data_freshness_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("data freshness summary production_effect must remain none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("data freshness summary must remain manual_review_only")
    if payload.get("data_freshness_only") is not True:
        raise ValueError("data freshness summary must remain data_freshness_only")
    if payload.get("read_only") is not True:
        raise ValueError("data freshness summary must remain read_only")
    if payload.get("data_downloaded_by_freshness_check") is not False:
        raise ValueError("data freshness summary must not download data")
    if payload.get("pipelines_executed_by_freshness_check") is not False:
        raise ValueError("data freshness summary must not execute pipelines")
    if payload.get("apply_executed_by_freshness_check") is not False:
        raise ValueError("data freshness summary must not execute apply")
    if payload.get("rollback_executed_by_freshness_check") is not False:
        raise ValueError("data freshness summary must not execute rollback")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"data freshness summary must keep {field}=false")
    if payload.get("safe_for_scheduler") is not True:
        raise ValueError("data freshness summary should be scheduler-safe")
