from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "daily_operator_brief_scheduler_dry_run"
RUN_REPORT_TYPE = "daily_operator_brief_scheduler_dry_run_log"
TASK_ID = "TRADING-026"
DIGEST_TASK_ID = "TRADING-021"
PIPELINE_HEALTH_TASK_ID = "TRADING-023"
DATA_FRESHNESS_TASK_ID = "TRADING-024"
OPERATOR_BRIEF_TASK_ID = "TRADING-022"
MODE = "daily_operator_brief_scheduler_dry_run_only"
PRODUCTION_EFFECT_NONE = "none"
DEFAULT_LOOKBACK_DAYS = 3
DEFAULT_FRESHNESS_DAYS = 2
DEFAULT_EXPECTED_RUN_HOUR = 9
DEFAULT_EXPECTED_RUN_MINUTE = 0
DEFAULT_TIMEZONE = "Asia/Tokyo"

DECISION_READY = "READY"
DECISION_READY_WITH_WARNINGS = "READY_WITH_WARNINGS"
DECISION_NOT_READY = "NOT_READY"
DECISION_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DECISION_ERROR = "ERROR"

STATUS_OK = "OK"
STATUS_WATCH = "WATCH"
STATUS_ACTION_REQUIRED = "ACTION_REQUIRED"
STATUS_SAFETY_BLOCKED = "SAFETY_BLOCKED"
STATUS_ERROR = "ERROR"

SUMMARY_NORMAL = "NORMAL"
SUMMARY_WATCH = "WATCH"
SUMMARY_ACTION = "ACTION"
SUMMARY_SAFETY_BLOCKED = "SAFETY_BLOCKED"
SUMMARY_ERROR = "ERROR"

ARTIFACT_FOUND = "FOUND"
ARTIFACT_MISSING = "MISSING"
ARTIFACT_INVALID = "INVALID"
FRESHNESS_FRESH = "FRESH"
FRESHNESS_STALE = "STALE"
FRESHNESS_MISSING = "MISSING"
FRESHNESS_UNKNOWN = "UNKNOWN"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

SCHEDULER_CONTRACT: dict[str, Any] = {
    "reads_existing_artifacts_only": True,
    "runs_daily_digest_script": False,
    "runs_pipeline_health_summary_script": False,
    "runs_data_freshness_summary_script": False,
    "runs_operator_brief_script": False,
    "creates_windows_task_scheduler_task": False,
    "creates_cron_job": False,
    "creates_github_actions_workflow": False,
    "runs_market_pipeline": False,
    "runs_backtest_pipeline": False,
    "runs_scoring_pipeline": False,
    "runs_data_download": False,
    "runs_broker_runner": False,
    "runs_replay_runner": False,
    "triggers_trade": False,
    "writes_production_profile": False,
    "writes_production_weights": False,
    "writes_shadow_weights": False,
    "writes_approved_profile": False,
    "promotes_shadow_to_production": False,
    "production_effect": PRODUCTION_EFFECT_NONE,
    "manual_review_only": True,
    "scheduler_dry_run_only": True,
    "read_only": True,
}


def default_scheduler_dry_run_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs" / "scheduler_dry_run"


def default_scheduler_dry_run_json_path(data_root: Path, as_of: date) -> Path:
    return default_scheduler_dry_run_root(data_root) / (
        f"daily_operator_brief_scheduler_dry_run_{as_of.isoformat()}.json"
    )


def default_scheduler_dry_run_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_scheduler_dry_run_root(data_root)
        / "logs"
        / f"daily_operator_brief_scheduler_dry_run_log_{as_of.isoformat()}.json"
    )


def default_parameter_governance_digest_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "governance" / "digests"


def default_pipeline_health_summary_root(data_root: Path) -> Path:
    return data_root / "derived" / "pipeline_health"


def default_data_freshness_summary_root(data_root: Path) -> Path:
    return data_root / "derived" / "data_freshness"


def default_operator_brief_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs"


def write_daily_operator_brief_scheduler_dry_run(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    expected_run_hour: int = DEFAULT_EXPECTED_RUN_HOUR,
    expected_run_minute: int = DEFAULT_EXPECTED_RUN_MINUTE,
    timezone: str = DEFAULT_TIMEZONE,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    strict: bool = False,
    fail_on_missing_required: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_scheduler_dry_run_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_scheduler_dry_run_run_log_json_path(
        data_root,
        as_of,
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")

    try:
        payload = build_daily_operator_brief_scheduler_dry_run_payload(
            as_of=as_of,
            data_root=data_root,
            expected_run_hour=expected_run_hour,
            expected_run_minute=expected_run_minute,
            timezone=timezone,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
            strict=strict,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
        )
    except Exception as exc:  # pragma: no cover - defensive artifact path
        payload = _error_payload(
            as_of=as_of,
            data_root=data_root,
            expected_run_hour=expected_run_hour,
            expected_run_minute=expected_run_minute,
            timezone=timezone,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
            strict=strict,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    _write_json(output_json_path, payload)
    _write_text(output_md_path, render_daily_operator_brief_scheduler_dry_run_markdown(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_daily_operator_brief_scheduler_dry_run_log(run_log))

    if fail_on_missing_required and _strings(
        _mapping(payload.get("dependency_check")).get("missing_required_inputs")
    ):
        raise SystemExit(2)
    return payload


def build_daily_operator_brief_scheduler_dry_run_payload(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    expected_run_hour: int = DEFAULT_EXPECTED_RUN_HOUR,
    expected_run_minute: int = DEFAULT_EXPECTED_RUN_MINUTE,
    timezone: str = DEFAULT_TIMEZONE,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    strict: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    _validate_schedule_inputs(
        expected_run_hour=expected_run_hour,
        expected_run_minute=expected_run_minute,
        lookback_days=lookback_days,
        freshness_days=freshness_days,
    )
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_scheduler_dry_run_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_scheduler_dry_run_run_log_json_path(
        data_root,
        as_of,
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")

    artifact_specs = _artifact_specs(data_root)
    artifact_states = {
        key: _artifact_state(
            key=key,
            spec=spec,
            as_of=as_of,
            lookback_days=lookback_days,
            freshness_days=freshness_days,
        )
        for key, spec in artifact_specs.items()
    }
    dependency_check = _dependency_check(artifact_states=artifact_states, strict=strict)
    safety_check = _safety_check(artifact_states)
    decision = _decision(
        dependency_check=dependency_check,
        safety_check=safety_check,
        strict=strict,
    )
    dry_run_status = _status_for_decision(decision)
    summary_level = _summary_level_for_decision(decision)
    alerts = _alerts(
        decision=decision,
        dependency_check=dependency_check,
        safety_check=safety_check,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "scheduler_dry_run_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "safe_for_scheduled_generation": decision in {DECISION_READY, DECISION_READY_WITH_WARNINGS},
        "scheduler_created": False,
        "operator_brief_executed_by_scheduler_dry_run": False,
        "pipelines_executed_by_scheduler_dry_run": False,
        "data_downloaded_by_scheduler_dry_run": False,
        "apply_executed_by_scheduler_dry_run": False,
        "rollback_executed_by_scheduler_dry_run": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "dry_run_decision": decision,
        "dry_run_status": dry_run_status,
        "summary_level": summary_level,
        "headline": _headline_for_decision(decision),
        "schedule_plan": {
            "intended_frequency": "DAILY",
            "expected_run_time_local": _time_label(expected_run_hour, expected_run_minute),
            "timezone": timezone,
            "scheduler_target": "manual_or_future_scheduler",
            "actual_scheduler_created": False,
        },
        "policy": {
            "strict": strict,
            "lookback_days": lookback_days,
            "freshness_days": freshness_days,
        },
        "input_artifacts": {
            key: _public_artifact_record(state) for key, state in artifact_states.items()
        },
        "dependency_check": dependency_check,
        "safety_check": safety_check,
        "expected_operator_brief_behavior": _expected_operator_brief_behavior(decision),
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(decision),
        "scheduler_contract": dict(SCHEDULER_CONTRACT),
        "output_artifacts": {
            "json": {"path": str(output_json_path)},
            "markdown": {"path": str(output_md_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
        "audit": {
            "created_by": "scripts/run_daily_operator_brief_scheduler_dry_run.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_scheduler_dry_run_artifacts": True,
        },
    }
    _assert_scheduler_dry_run_safety_invariants(payload)
    return payload


def render_daily_operator_brief_scheduler_dry_run_markdown(payload: dict[str, Any]) -> str:
    decision = _string_value(payload.get("dry_run_decision"))
    banner = {
        DECISION_SAFETY_BLOCKED: "## Scheduler Dry Run Safety Blocked",
        DECISION_NOT_READY: "## Scheduler Dry Run Not Ready",
        DECISION_READY_WITH_WARNINGS: "## Scheduler Dry Run Ready With Warnings",
        DECISION_ERROR: "## Scheduler Dry Run Error",
    }.get(decision)
    schedule = _mapping(payload.get("schedule_plan"))
    dependency = _mapping(payload.get("dependency_check"))
    safety = _mapping(payload.get("safety_check"))
    behavior = _mapping(payload.get("expected_operator_brief_behavior"))
    alerts = _mapping(payload.get("alerts"))
    lines = [
        f"# Daily Operator Brief Scheduler Dry Run - {payload.get('date')}",
        "",
    ]
    if banner:
        lines.extend([banner, ""])
    lines.extend(
        [
            "## 1. Dry Run Summary",
            "",
            f"- Dry Run Decision: `{payload.get('dry_run_decision')}`",
            f"- Dry Run Status: `{payload.get('dry_run_status')}`",
            f"- Summary Level: `{payload.get('summary_level')}`",
            f"- Scheduler Created: `{_bool_text(payload.get('scheduler_created'))}`",
            "- Operator Brief Executed By Dry Run: "
            f"`{_bool_text(payload.get('operator_brief_executed_by_scheduler_dry_run'))}`",
            "- Pipelines Executed By Dry Run: "
            f"`{_bool_text(payload.get('pipelines_executed_by_scheduler_dry_run'))}`",
            "- Data Downloaded By Dry Run: "
            f"`{_bool_text(payload.get('data_downloaded_by_scheduler_dry_run'))}`",
            f"- Broker Execution: `{_bool_text(payload.get('broker_execution'))}`",
            f"- Replay Execution: `{_bool_text(payload.get('replay_execution'))}`",
            f"- Trading Execution: `{_bool_text(payload.get('trading_execution'))}`",
            "",
            "## 2. Schedule Plan",
            "",
            f"- Intended Frequency: `{schedule.get('intended_frequency')}`",
            f"- Expected Run Time Local: `{schedule.get('expected_run_time_local')}`",
            f"- Timezone: `{schedule.get('timezone')}`",
            "- Actual Scheduler Created: "
            f"`{_bool_text(schedule.get('actual_scheduler_created'))}`",
            "",
            "## 3. Dependency Check",
            "",
            "| Input | Required | Status | Freshness | Path |",
            "|---|---:|---:|---:|---|",
        ]
    )
    artifacts = _mapping(payload.get("input_artifacts"))
    labels = {
        "parameter_governance_daily_digest": "TRADING-021 Digest",
        "pipeline_health_summary": "TRADING-023 Pipeline Health",
        "data_freshness_summary": "TRADING-024 Data Freshness",
        "existing_operator_brief": "Existing TRADING-022 Brief",
    }
    for key, label in labels.items():
        artifact = _mapping(artifacts.get(key))
        lines.append(
            "| "
            + " | ".join(
                [
                    _table_text(label),
                    str(artifact.get("required") is True).lower(),
                    _table_text(artifact.get("status")),
                    _table_text(artifact.get("freshness")),
                    _table_text(artifact.get("path")),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## 4. Safety Check",
            "",
            "| Check | Status | Notes |",
            "|---|---:|---|",
        ]
    )
    for check in (
        "no_scheduler_created",
        "no_pipeline_execution",
        "no_data_download",
        "no_broker_execution",
        "no_replay_execution",
        "no_trading_execution",
    ):
        value = safety.get(check)
        lines.append(
            f"| `{check}` | `{'PASS' if value is True else 'FAIL'}` | " f"{_table_text(value)} |"
        )
    lines.extend(
        [
            "",
            "## 5. Expected Operator Brief Behavior",
            "",
            "- Would Execute Operator Brief: "
            f"`{_bool_text(behavior.get('would_execute_operator_brief'))}`",
            f"- Expected If Operator Brief Ran: `{behavior.get('expected_if_operator_brief_ran')}`",
            f"- Expected Degradation: `{behavior.get('expected_degradation')}`",
            "- Notes:",
            *_markdown_bullets(_strings(behavior.get("notes"))),
            "",
            "## 6. Alerts",
            "",
            "### Critical",
            "",
            *_markdown_bullets(_strings(alerts.get("critical"))),
            "",
            "### Warnings",
            "",
            *_markdown_bullets(_strings(alerts.get("warnings"))),
            "",
            "### Notes",
            "",
            *_markdown_bullets(_strings(alerts.get("notes"))),
            "",
            "## 7. Recommended Next Steps",
            "",
            *_markdown_bullets(_strings(payload.get("recommended_next_steps"))),
            "",
            "## Audit",
            "",
            f"- production_effect: `{payload.get('production_effect')}`",
            f"- manual_review_only: `{_bool_text(payload.get('manual_review_only'))}`",
            f"- scheduler_dry_run_only: `{_bool_text(payload.get('scheduler_dry_run_only'))}`",
            f"- read_only: `{_bool_text(payload.get('read_only'))}`",
            f"- safe_for_scheduler: `{_bool_text(payload.get('safe_for_scheduler'))}`",
            f"- dependency_check.status: `{dependency.get('status')}`",
            f"- safety_check.status: `{safety.get('status')}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_daily_operator_brief_scheduler_dry_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Daily Operator Brief Scheduler Dry Run Log - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- dry_run_decision: `{payload.get('dry_run_decision')}`",
            f"- dry_run_status: `{payload.get('dry_run_status')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- scheduler_dry_run_only: `true`",
            "- read_only: `true`",
            "- scheduler_created: `false`",
            "- operator_brief_executed_by_scheduler_dry_run: `false`",
            "- pipelines_executed_by_scheduler_dry_run: `false`",
            "- data_downloaded_by_scheduler_dry_run: `false`",
            "- apply_executed_by_scheduler_dry_run: `false`",
            "- rollback_executed_by_scheduler_dry_run: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- scheduler_dry_run_json: `{payload.get('scheduler_dry_run_json')}`",
            f"- scheduler_dry_run_markdown: `{payload.get('scheduler_dry_run_markdown')}`",
            "",
        ]
    )


def _artifact_specs(data_root: Path) -> dict[str, dict[str, Any]]:
    return {
        "parameter_governance_daily_digest": {
            "required": True,
            "root": default_parameter_governance_digest_root(data_root),
            "prefix": "parameter_governance_daily_digest_",
            "default_path": default_parameter_governance_digest_root(data_root)
            / "parameter_governance_daily_digest_{date}.json",
            "report_type": "parameter_governance_daily_digest",
            "task_id": DIGEST_TASK_ID,
            "safety_checks": {
                "task_id": DIGEST_TASK_ID,
                "production_effect": PRODUCTION_EFFECT_NONE,
                "digest_only": True,
                "governance_only": True,
                "apply_executed_by_digest": False,
                "rollback_executed_by_digest": False,
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
            },
        },
        "pipeline_health_summary": {
            "required": False,
            "root": default_pipeline_health_summary_root(data_root),
            "prefix": "pipeline_health_summary_",
            "default_path": default_pipeline_health_summary_root(data_root)
            / "pipeline_health_summary_{date}.json",
            "report_type": "pipeline_health_summary",
            "task_id": PIPELINE_HEALTH_TASK_ID,
            "safety_checks": {
                "task_id": PIPELINE_HEALTH_TASK_ID,
                "production_effect": PRODUCTION_EFFECT_NONE,
                "pipeline_health_only": True,
                "read_only": True,
                "pipelines_executed_by_health_check": False,
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
            },
        },
        "data_freshness_summary": {
            "required": False,
            "root": default_data_freshness_summary_root(data_root),
            "prefix": "data_freshness_summary_",
            "default_path": default_data_freshness_summary_root(data_root)
            / "data_freshness_summary_{date}.json",
            "report_type": "data_freshness_summary",
            "task_id": DATA_FRESHNESS_TASK_ID,
            "safety_checks": {
                "task_id": DATA_FRESHNESS_TASK_ID,
                "production_effect": PRODUCTION_EFFECT_NONE,
                "data_freshness_only": True,
                "read_only": True,
                "data_downloaded_by_freshness_check": False,
                "pipelines_executed_by_freshness_check": False,
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
            },
        },
        "existing_operator_brief": {
            "required": False,
            "root": default_operator_brief_root(data_root),
            "prefix": "daily_trading_system_operator_brief_",
            "default_path": default_operator_brief_root(data_root)
            / "daily_trading_system_operator_brief_{date}.json",
            "report_type": "daily_trading_system_operator_brief",
            "task_id": OPERATOR_BRIEF_TASK_ID,
            "safety_checks": {
                "task_id": OPERATOR_BRIEF_TASK_ID,
                "production_effect": PRODUCTION_EFFECT_NONE,
                "operator_brief_only": True,
                "read_only": True,
                "apply_executed_by_operator_brief": False,
                "rollback_executed_by_operator_brief": False,
                "broker_execution": False,
                "replay_execution": False,
                "trading_execution": False,
            },
        },
    }


def _artifact_state(
    *,
    key: str,
    spec: dict[str, Any],
    as_of: date,
    lookback_days: int,
    freshness_days: int,
) -> dict[str, Any]:
    path = _latest_dated_artifact(
        root=Path(str(spec["root"])),
        prefix=str(spec["prefix"]),
        as_of=as_of,
        lookback_days=lookback_days,
        default_path=Path(str(spec["default_path"]).format(date=as_of.isoformat())),
    )
    exists = path.exists() and path.is_file()
    status = ARTIFACT_FOUND if exists else ARTIFACT_MISSING
    payload: dict[str, Any] = {}
    read_error = ""
    if exists:
        payload, read_error = _read_json_object_with_error(path)
        if read_error:
            status = ARTIFACT_INVALID
    artifact_date = _artifact_date(path=path, payload=payload)
    freshness = _freshness_status(
        exists=exists,
        status=status,
        artifact_date=artifact_date,
        as_of=as_of,
        freshness_days=freshness_days,
    )
    safety = _artifact_safety_validation(
        key=key,
        spec=spec,
        payload=payload,
        exists=exists,
        status=status,
        read_error=read_error,
    )
    return {
        "key": key,
        "required": spec.get("required") is True,
        "path": path,
        "exists": exists,
        "status": status,
        "sha256": _sha256(path) if exists and status != ARTIFACT_INVALID else None,
        "size_bytes": path.stat().st_size if exists else 0,
        "artifact_date": artifact_date,
        "freshness": freshness,
        "read_error": read_error,
        "payload": payload,
        "safety": safety,
    }


def _latest_dated_artifact(
    *,
    root: Path,
    prefix: str,
    as_of: date,
    lookback_days: int,
    default_path: Path,
) -> Path:
    earliest = as_of - timedelta(days=lookback_days)
    candidates: list[tuple[date, Path]] = []
    if root.exists():
        for path in root.glob(f"{prefix}*.json"):
            parsed = _date_from_artifact_name(path)
            if parsed is not None and earliest <= parsed <= as_of:
                candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _artifact_safety_validation(
    *,
    key: str,
    spec: dict[str, Any],
    payload: dict[str, Any],
    exists: bool,
    status: str,
    read_error: str,
) -> dict[str, Any]:
    if not exists:
        return {"status": "NOT_EVALUATED", "safe": True, "blocking_reasons": []}
    if status == ARTIFACT_INVALID:
        return {
            "status": "NOT_EVALUATED",
            "safe": True,
            "blocking_reasons": [read_error] if read_error else [],
        }

    checks = _mapping(spec.get("safety_checks"))
    blocking_reasons = []
    for field, expected in checks.items():
        if payload.get(field) != expected:
            blocking_reasons.append(
                f"{key}.{field} must be {_expected_value_text(expected)}; "
                f"got {_expected_value_text(payload.get(field))}."
            )
    return {
        "status": "PASS" if not blocking_reasons else "FAIL",
        "safe": not blocking_reasons,
        "blocking_reasons": blocking_reasons,
    }


def _dependency_check(
    *,
    artifact_states: dict[str, dict[str, Any]],
    strict: bool,
) -> dict[str, Any]:
    missing_required: list[str] = []
    missing_optional: list[str] = []
    stale_inputs: list[str] = []
    invalid_inputs: list[str] = []
    blocking_reasons: list[str] = []

    for key in ("parameter_governance_daily_digest",):
        state = artifact_states[key]
        if state["status"] == ARTIFACT_MISSING:
            missing_required.append(key)
            blocking_reasons.append(f"Required input missing: {key}.")
        elif state["status"] == ARTIFACT_INVALID:
            invalid_inputs.append(key)
            blocking_reasons.append(f"Required input invalid: {key}.")
        elif state["freshness"] == FRESHNESS_STALE:
            stale_inputs.append(key)
            blocking_reasons.append(f"Required input stale: {key}.")

    for key in ("pipeline_health_summary", "data_freshness_summary"):
        state = artifact_states[key]
        if state["status"] == ARTIFACT_MISSING:
            missing_optional.append(key)
            if strict:
                blocking_reasons.append(f"Strict mode optional input missing: {key}.")
        elif state["status"] == ARTIFACT_INVALID:
            invalid_inputs.append(key)
            if strict:
                blocking_reasons.append(f"Strict mode optional input invalid: {key}.")
        elif state["freshness"] == FRESHNESS_STALE:
            stale_inputs.append(key)
            if strict:
                blocking_reasons.append(f"Strict mode optional input stale: {key}.")

    existing = artifact_states["existing_operator_brief"]
    if existing["status"] == ARTIFACT_INVALID:
        invalid_inputs.append("existing_operator_brief")
        if strict:
            blocking_reasons.append("Strict mode existing operator brief invalid.")
    elif existing["status"] == ARTIFACT_FOUND and existing["freshness"] == FRESHNESS_STALE:
        stale_inputs.append("existing_operator_brief")

    required_available = not missing_required and not any(
        item in {"parameter_governance_daily_digest"} for item in invalid_inputs + stale_inputs
    )
    optional_available = not missing_optional and not any(
        item in {"pipeline_health_summary", "data_freshness_summary"}
        for item in invalid_inputs + stale_inputs
    )
    return {
        "status": "PASS" if not blocking_reasons else "FAIL",
        "required_inputs_available": required_available,
        "optional_inputs_available": optional_available,
        "missing_required_inputs": missing_required,
        "missing_optional_inputs": missing_optional,
        "invalid_inputs": invalid_inputs,
        "stale_inputs": stale_inputs,
        "blocking_reasons": blocking_reasons,
    }


def _safety_check(artifact_states: dict[str, dict[str, Any]]) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    safety_by_key: dict[str, bool] = {}
    for key, state in artifact_states.items():
        safety = _mapping(state.get("safety"))
        safety_by_key[key] = safety.get("safe") is True
        blocking_reasons.extend(
            _strings(safety.get("blocking_reasons")) if not safety_by_key[key] else []
        )

    no_scheduler_created = True
    no_pipeline_execution = True
    no_data_download = True
    no_broker_execution = True
    no_replay_execution = True
    no_trading_execution = True
    return {
        "status": "PASS" if not blocking_reasons else "FAIL",
        "digest_safe": safety_by_key.get("parameter_governance_daily_digest", True),
        "pipeline_health_safe": safety_by_key.get("pipeline_health_summary", True),
        "data_freshness_safe": safety_by_key.get("data_freshness_summary", True),
        "existing_operator_brief_safe": safety_by_key.get("existing_operator_brief", True),
        "no_scheduler_created": no_scheduler_created,
        "no_pipeline_execution": no_pipeline_execution,
        "no_data_download": no_data_download,
        "no_broker_execution": no_broker_execution,
        "no_replay_execution": no_replay_execution,
        "no_trading_execution": no_trading_execution,
        "blocking_reasons": blocking_reasons,
    }


def _decision(
    *,
    dependency_check: dict[str, Any],
    safety_check: dict[str, Any],
    strict: bool,
) -> str:
    if safety_check.get("status") != "PASS":
        return DECISION_SAFETY_BLOCKED
    if dependency_check.get("required_inputs_available") is not True:
        return DECISION_NOT_READY
    if strict and dependency_check.get("status") != "PASS":
        return DECISION_NOT_READY
    if (
        _strings(dependency_check.get("missing_optional_inputs"))
        or _optional_stale_inputs(dependency_check)
        or _optional_invalid_inputs(dependency_check)
    ):
        return DECISION_READY_WITH_WARNINGS
    return DECISION_READY


def _optional_stale_inputs(dependency_check: dict[str, Any]) -> list[str]:
    return [
        item
        for item in _strings(dependency_check.get("stale_inputs"))
        if item in {"pipeline_health_summary", "data_freshness_summary", "existing_operator_brief"}
    ]


def _optional_invalid_inputs(dependency_check: dict[str, Any]) -> list[str]:
    return [
        item
        for item in _strings(dependency_check.get("invalid_inputs"))
        if item in {"pipeline_health_summary", "data_freshness_summary", "existing_operator_brief"}
    ]


def _status_for_decision(decision: str) -> str:
    return {
        DECISION_READY: STATUS_OK,
        DECISION_READY_WITH_WARNINGS: STATUS_WATCH,
        DECISION_NOT_READY: STATUS_ACTION_REQUIRED,
        DECISION_SAFETY_BLOCKED: STATUS_SAFETY_BLOCKED,
        DECISION_ERROR: STATUS_ERROR,
    }[decision]


def _summary_level_for_decision(decision: str) -> str:
    return {
        DECISION_READY: SUMMARY_NORMAL,
        DECISION_READY_WITH_WARNINGS: SUMMARY_WATCH,
        DECISION_NOT_READY: SUMMARY_ACTION,
        DECISION_SAFETY_BLOCKED: SUMMARY_SAFETY_BLOCKED,
        DECISION_ERROR: SUMMARY_ERROR,
    }[decision]


def _headline_for_decision(decision: str) -> str:
    return {
        DECISION_READY: (
            "Daily operator brief scheduler dry run is ready. Required inputs are "
            "available and no safety blockers were detected."
        ),
        DECISION_READY_WITH_WARNINGS: (
            "Daily operator brief scheduler dry run is ready with warnings. Required "
            "inputs are available, but optional health or freshness context is missing "
            "or stale."
        ),
        DECISION_NOT_READY: (
            "Daily operator brief scheduler dry run is not ready. Required input "
            "artifacts are missing, invalid, stale, or strict dependency checks failed."
        ),
        DECISION_SAFETY_BLOCKED: (
            "Daily operator brief scheduler dry run is safety blocked. At least one "
            "input artifact violates read-only or no-execution safety fields."
        ),
        DECISION_ERROR: "Daily operator brief scheduler dry run failed with an error.",
    }[decision]


def _expected_operator_brief_behavior(decision: str) -> dict[str, Any]:
    expected_if_ran = {
        DECISION_READY: STATUS_OK,
        DECISION_READY_WITH_WARNINGS: STATUS_WATCH,
        DECISION_NOT_READY: "INPUT_MISSING",
        DECISION_SAFETY_BLOCKED: STATUS_SAFETY_BLOCKED,
        DECISION_ERROR: STATUS_ERROR,
    }[decision]
    degradation = {
        DECISION_READY: "NONE",
        DECISION_READY_WITH_WARNINGS: STATUS_WATCH,
        DECISION_NOT_READY: STATUS_ACTION_REQUIRED,
        DECISION_SAFETY_BLOCKED: STATUS_SAFETY_BLOCKED,
        DECISION_ERROR: STATUS_ERROR,
    }[decision]
    return {
        "would_execute_operator_brief": False,
        "expected_if_operator_brief_ran": expected_if_ran,
        "expected_degradation": degradation,
        "notes": [
            "This dry run does not execute TRADING-022. It only checks whether inputs are ready."
        ],
    }


def _alerts(
    *,
    decision: str,
    dependency_check: dict[str, Any],
    safety_check: dict[str, Any],
) -> dict[str, list[str]]:
    critical: list[str] = []
    warnings: list[str] = []
    if decision == DECISION_SAFETY_BLOCKED:
        critical.extend(_strings(safety_check.get("blocking_reasons")))
    if decision in {DECISION_READY_WITH_WARNINGS, DECISION_NOT_READY}:
        warnings.extend(_strings(dependency_check.get("blocking_reasons")))
        for item in _strings(dependency_check.get("missing_optional_inputs")):
            warnings.append(f"Optional input missing: {item}.")
        for item in _optional_stale_inputs(dependency_check):
            warnings.append(f"Optional input stale: {item}.")
        for item in _optional_invalid_inputs(dependency_check):
            warnings.append(f"Optional input invalid: {item}.")
    return {
        "critical": critical,
        "warnings": list(dict.fromkeys(warnings)),
        "notes": ["Scheduler dry run is read-only."],
    }


def _recommended_next_steps(decision: str) -> list[str]:
    base = [
        "Manual operator brief generation may be run if needed.",
        "Do not create a real scheduler until this dry run is repeatedly stable.",
    ]
    if decision == DECISION_NOT_READY:
        return [
            "Regenerate or locate the required TRADING-021 digest through its own runbook.",
            *base,
        ]
    if decision == DECISION_READY_WITH_WARNINGS:
        return [
            "Review optional TRADING-023 and TRADING-024 inputs before relying on "
            "scheduled generation.",
            *base,
        ]
    if decision == DECISION_SAFETY_BLOCKED:
        return [
            "Do not schedule operator brief generation until unsafe input artifacts are "
            "investigated.",
            *base,
        ]
    return base


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    output_artifacts = _mapping(payload.get("output_artifacts"))
    scheduler_json = _string_value(_mapping(output_artifacts.get("json")).get("path"))
    scheduler_markdown = _string_value(_mapping(output_artifacts.get("markdown")).get("path"))
    run_log_json = _string_value(_mapping(output_artifacts.get("run_log_json")).get("path"))
    run_log_markdown = _string_value(_mapping(output_artifacts.get("run_log_markdown")).get("path"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "created_at": _isoformat_z(generated_at),
        "run_status": payload.get("dry_run_status"),
        "dry_run_decision": payload.get("dry_run_decision"),
        "dry_run_status": payload.get("dry_run_status"),
        "summary_level": payload.get("summary_level"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "scheduler_dry_run_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "scheduler_created": False,
        "operator_brief_executed_by_scheduler_dry_run": False,
        "pipelines_executed_by_scheduler_dry_run": False,
        "data_downloaded_by_scheduler_dry_run": False,
        "apply_executed_by_scheduler_dry_run": False,
        "rollback_executed_by_scheduler_dry_run": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "scheduler_dry_run_json": scheduler_json,
        "scheduler_dry_run_markdown": scheduler_markdown,
        "run_log_json": run_log_json,
        "run_log_markdown": run_log_markdown,
        "dependency_check_status": _mapping(payload.get("dependency_check")).get("status"),
        "safety_check_status": _mapping(payload.get("safety_check")).get("status"),
    }


def _error_payload(
    *,
    as_of: date,
    data_root: Path,
    expected_run_hour: int,
    expected_run_minute: int,
    timezone: str,
    lookback_days: int,
    freshness_days: int,
    strict: bool,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "scheduler_dry_run_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "safe_for_scheduled_generation": False,
        "scheduler_created": False,
        "operator_brief_executed_by_scheduler_dry_run": False,
        "pipelines_executed_by_scheduler_dry_run": False,
        "data_downloaded_by_scheduler_dry_run": False,
        "apply_executed_by_scheduler_dry_run": False,
        "rollback_executed_by_scheduler_dry_run": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "dry_run_decision": DECISION_ERROR,
        "dry_run_status": STATUS_ERROR,
        "summary_level": SUMMARY_ERROR,
        "headline": _headline_for_decision(DECISION_ERROR),
        "schedule_plan": {
            "intended_frequency": "DAILY",
            "expected_run_time_local": _safe_time_label(expected_run_hour, expected_run_minute),
            "timezone": timezone,
            "scheduler_target": "manual_or_future_scheduler",
            "actual_scheduler_created": False,
        },
        "policy": {
            "strict": strict,
            "lookback_days": lookback_days,
            "freshness_days": freshness_days,
        },
        "input_artifacts": {},
        "dependency_check": {
            "status": "FAIL",
            "required_inputs_available": False,
            "optional_inputs_available": False,
            "missing_required_inputs": [],
            "missing_optional_inputs": [],
            "invalid_inputs": [],
            "stale_inputs": [],
            "blocking_reasons": [error],
        },
        "safety_check": {
            "status": "PASS",
            "digest_safe": True,
            "pipeline_health_safe": True,
            "data_freshness_safe": True,
            "existing_operator_brief_safe": True,
            "no_scheduler_created": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_broker_execution": True,
            "no_replay_execution": True,
            "no_trading_execution": True,
            "blocking_reasons": [],
        },
        "expected_operator_brief_behavior": _expected_operator_brief_behavior(DECISION_ERROR),
        "alerts": {"critical": [], "warnings": [], "notes": [error]},
        "recommended_next_steps": ["Inspect the scheduler dry-run run log."],
        "scheduler_contract": dict(SCHEDULER_CONTRACT),
        "output_artifacts": {
            "json": {"path": str(output_json_path)},
            "markdown": {"path": str(output_md_path)},
            "run_log_json": {"path": str(run_log_json_path)},
            "run_log_markdown": {"path": str(run_log_md_path)},
        },
        "audit": {
            "created_by": "scripts/run_daily_operator_brief_scheduler_dry_run.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_scheduler_dry_run_artifacts": True,
            "error": error,
            "data_root": str(data_root),
        },
    }


def _public_artifact_record(state: dict[str, Any]) -> dict[str, Any]:
    artifact_date = state.get("artifact_date")
    return {
        "status": state.get("status"),
        "required": state.get("required") is True,
        "path": str(state.get("path")),
        "sha256": state.get("sha256"),
        "freshness": state.get("freshness"),
        "artifact_date": artifact_date.isoformat() if isinstance(artifact_date, date) else None,
        "safety_status": _mapping(state.get("safety")).get("status"),
        "blocking_reasons": _strings(_mapping(state.get("safety")).get("blocking_reasons")),
    }


def _freshness_status(
    *,
    exists: bool,
    status: str,
    artifact_date: date | None,
    as_of: date,
    freshness_days: int,
) -> str:
    if not exists:
        return FRESHNESS_MISSING
    if status == ARTIFACT_INVALID:
        return FRESHNESS_UNKNOWN
    if artifact_date is None:
        return FRESHNESS_UNKNOWN
    if artifact_date > as_of:
        return FRESHNESS_STALE
    return FRESHNESS_STALE if (as_of - artifact_date).days > freshness_days else FRESHNESS_FRESH


def _artifact_date(*, path: Path, payload: dict[str, Any]) -> date | None:
    json_date = _parse_date_value(payload.get("date"))
    if json_date is not None:
        return json_date
    filename_date = _date_from_artifact_name(path)
    if filename_date is not None:
        return filename_date
    if path.exists():
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()
    return None


def _date_from_artifact_name(path: Path) -> date | None:
    matches = re.findall(r"\d{4}[-_]\d{2}[-_]\d{2}", path.name)
    parsed = [_parse_date_value(value) for value in matches]
    dates = [value for value in parsed if value is not None]
    return dates[-1] if dates else None


def _parse_date_value(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value.replace("_", "-")[:10])
    except ValueError:
        return None


def _read_json_object_with_error(path: Path) -> tuple[dict[str, Any], str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, f"Artifact JSON is invalid: {exc}"
    except OSError as exc:
        return {}, f"Artifact cannot be read: {exc}"
    if not isinstance(payload, dict):
        return {}, "Artifact JSON must be an object."
    return payload, ""


def _validate_schedule_inputs(
    *,
    expected_run_hour: int,
    expected_run_minute: int,
    lookback_days: int,
    freshness_days: int,
) -> None:
    if not 0 <= expected_run_hour <= 23:
        raise ValueError("expected_run_hour must be between 0 and 23")
    if not 0 <= expected_run_minute <= 59:
        raise ValueError("expected_run_minute must be between 0 and 59")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if freshness_days < 0:
        raise ValueError("freshness_days must be non-negative")


def _time_label(hour: int, minute: int) -> str:
    return f"{hour:02d}:{minute:02d}"


def _safe_time_label(hour: int, minute: int) -> str:
    try:
        return _time_label(hour, minute)
    except ValueError:
        return "invalid"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _assert_scheduler_dry_run_safety_invariants(payload: dict[str, Any]) -> None:
    expected_true = (
        "manual_review_only",
        "scheduler_dry_run_only",
        "read_only",
        "safe_for_scheduler",
    )
    expected_false = (
        "scheduler_created",
        "operator_brief_executed_by_scheduler_dry_run",
        "pipelines_executed_by_scheduler_dry_run",
        "data_downloaded_by_scheduler_dry_run",
        "apply_executed_by_scheduler_dry_run",
        "rollback_executed_by_scheduler_dry_run",
        "broker_execution",
        "replay_execution",
        "trading_execution",
    )
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("scheduler dry-run production_effect must remain none")
    for field in expected_true:
        if payload.get(field) is not True:
            raise ValueError(f"scheduler dry-run must keep {field}=true")
    for field in expected_false:
        if payload.get(field) is not False:
            raise ValueError(f"scheduler dry-run must keep {field}=false")


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _bool_text(value: Any) -> str:
    return str(value is True).lower()


def _expected_value_text(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _markdown_bullets(items: list[str]) -> list[str]:
    return [f"- {item}" for item in items] or ["- None."]


def _table_text(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")
