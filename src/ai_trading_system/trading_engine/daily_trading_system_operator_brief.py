from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "daily_trading_system_operator_brief"
RUN_REPORT_TYPE = "daily_trading_system_operator_brief_run"
TASK_ID = "TRADING-022"
DIGEST_TASK_ID = "TRADING-021"
PIPELINE_HEALTH_TASK_ID = "TRADING-023"
DATA_FRESHNESS_TASK_ID = "TRADING-024"
MODE = "daily_trading_system_operator_brief_only"
PRODUCTION_EFFECT_NONE = "none"
DEFAULT_LOOKBACK_DAYS = 7

BRIEF_OK = "OK"
BRIEF_WATCH = "WATCH"
BRIEF_ACTION_REQUIRED = "ACTION_REQUIRED"
BRIEF_URGENT = "URGENT"
BRIEF_INPUT_MISSING = "INPUT_MISSING"
BRIEF_INPUT_INVALID = "INPUT_INVALID"
BRIEF_SAFETY_BLOCKED = "SAFETY_BLOCKED"
BRIEF_ERROR = "ERROR"

SUMMARY_NORMAL = "NORMAL"
SUMMARY_WATCH = "WATCH"
SUMMARY_ACTION = "ACTION"
SUMMARY_URGENT = "URGENT"
SUMMARY_UNKNOWN = "UNKNOWN"

OPTIONAL_NOT_FOUND = "OPTIONAL_NOT_FOUND"
STATUS_UNKNOWN = "UNKNOWN"
STATUS_AVAILABLE = "AVAILABLE"
STATUS_FOUND = "FOUND"

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
    "runs_operator_brief_script": False,
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
    "operator_brief_only": True,
    "read_only": True,
}


def default_operator_brief_root(data_root: Path) -> Path:
    return data_root / "derived" / "operator_briefs"


def default_operator_brief_json_path(data_root: Path, as_of: date) -> Path:
    return default_operator_brief_root(data_root) / (
        f"daily_trading_system_operator_brief_{as_of.isoformat()}.json"
    )


def default_operator_brief_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_operator_brief_root(data_root)
        / "logs"
        / f"daily_trading_system_operator_brief_run_{as_of.isoformat()}.json"
    )


def default_parameter_governance_digest_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "governance" / "digests"


def default_parameter_governance_digest_json_path(data_root: Path, as_of: date) -> Path:
    return default_parameter_governance_digest_root(data_root) / (
        f"parameter_governance_daily_digest_{as_of.isoformat()}.json"
    )


def default_pipeline_health_summary_root(data_root: Path) -> Path:
    return data_root / "derived" / "pipeline_health"


def default_pipeline_health_summary_json_path(data_root: Path, as_of: date) -> Path:
    return default_pipeline_health_summary_root(data_root) / (
        f"pipeline_health_summary_{as_of.isoformat()}.json"
    )


def default_data_freshness_summary_root(data_root: Path) -> Path:
    return data_root / "derived" / "data_freshness"


def default_data_freshness_summary_json_path(data_root: Path, as_of: date) -> Path:
    return default_data_freshness_summary_root(data_root) / (
        f"data_freshness_summary_{as_of.isoformat()}.json"
    )


def write_daily_trading_system_operator_brief(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    parameter_governance_digest_file: Path | None = None,
    pipeline_health_summary_file: Path | None = None,
    data_freshness_summary_file: Path | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    fail_on_critical: bool = False,
    include_optional_artifacts: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_operator_brief_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_operator_brief_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_daily_trading_system_operator_brief_payload(
            as_of=as_of,
            data_root=data_root,
            parameter_governance_digest_file=parameter_governance_digest_file,
            pipeline_health_summary_file=pipeline_health_summary_file,
            data_freshness_summary_file=data_freshness_summary_file,
            lookback_days=lookback_days,
            include_optional_artifacts=include_optional_artifacts,
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
            parameter_governance_digest_file=parameter_governance_digest_file,
            pipeline_health_summary_file=pipeline_health_summary_file,
            data_freshness_summary_file=data_freshness_summary_file,
            lookback_days=lookback_days,
            include_optional_artifacts=include_optional_artifacts,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    _write_json(output_json_path, payload)
    _write_text(output_md_path, render_daily_trading_system_operator_brief_markdown(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_daily_trading_system_operator_brief_run_log(run_log))

    if fail_on_critical and _strings(_mapping(payload.get("alerts")).get("critical")):
        raise SystemExit(2)
    return payload


def build_daily_trading_system_operator_brief_payload(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    parameter_governance_digest_file: Path | None = None,
    pipeline_health_summary_file: Path | None = None,
    data_freshness_summary_file: Path | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    include_optional_artifacts: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_operator_brief_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_operator_brief_run_log_json_path(
        data_root, as_of
    )
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")

    digest_path = _resolve_digest_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=parameter_governance_digest_file,
        lookback_days=lookback_days,
    )
    digest_payload, input_status, input_reason = _load_digest(digest_path)
    digest_safety_validation = _digest_safety_validation(
        digest_payload,
        input_status=input_status,
        input_reason=input_reason,
    )

    pipeline_health = _pipeline_health_status(
        as_of=as_of,
        data_root=data_root,
        explicit_path=pipeline_health_summary_file,
        lookback_days=lookback_days,
    )
    data_freshness = _data_freshness_status(
        as_of=as_of,
        data_root=data_root,
        explicit_path=data_freshness_summary_file,
        lookback_days=lookback_days,
    )
    safety_validation = _input_safety_validation(
        digest_validation=digest_safety_validation,
        pipeline_health=pipeline_health,
        data_freshness=data_freshness,
    )
    market_report = _market_report_status(
        as_of=as_of,
        data_root=data_root,
        lookback_days=lookback_days,
    )
    weight_iteration = _weight_iteration_status(
        as_of=as_of,
        data_root=data_root,
        lookback_days=lookback_days,
        include_optional_artifacts=include_optional_artifacts,
    )

    alerts = _alerts(
        digest=digest_payload,
        input_status=input_status,
        input_reason=input_reason,
        safety_validation=safety_validation,
        pipeline_health=pipeline_health,
        data_freshness=data_freshness,
    )
    parameter_governance = _parameter_governance_status(
        digest=digest_payload,
        input_status=input_status,
        safety_validation=digest_safety_validation,
    )
    pending_manual_actions = _pending_manual_actions(
        digest=digest_payload,
        brief_alerts=alerts,
        input_status=input_status,
        safety_validation=safety_validation,
        pipeline_health=pipeline_health,
        data_freshness=data_freshness,
    )
    brief_status = _brief_status(
        digest=digest_payload,
        input_status=input_status,
        safety_validation=safety_validation,
        pipeline_health=pipeline_health,
        data_freshness=data_freshness,
        alerts=alerts,
        pending_manual_actions=pending_manual_actions,
    )
    summary_level = _summary_level(brief_status)
    headline = _headline(brief_status)
    system_snapshot = _system_snapshot(
        brief_status=brief_status,
        alerts=alerts,
        pending_manual_actions=pending_manual_actions,
    )
    links = _links(
        digest=digest_payload,
        data_root=data_root,
        as_of=as_of,
        output_json_path=output_json_path,
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
        "operator_brief_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "apply_executed_by_operator_brief": False,
        "rollback_executed_by_operator_brief": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "brief_status": brief_status,
        "summary_level": summary_level,
        "headline": headline,
        "input_artifacts": {
            "parameter_governance_daily_digest": _artifact_record(digest_path),
            "pipeline_health_summary": pipeline_health["artifact"],
            "data_freshness_summary": data_freshness["artifact"],
            "market_report": market_report["artifact"],
        },
        "system_snapshot": system_snapshot,
        "parameter_governance": parameter_governance,
        "pipeline_health": _without_artifact(pipeline_health),
        "data_freshness": _without_artifact(data_freshness),
        "market_report_status": _without_artifact(market_report),
        "weight_iteration_status": weight_iteration,
        "pending_manual_actions": pending_manual_actions,
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(brief_status),
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
            "created_by": "scripts/run_daily_trading_system_operator_brief.py",
            "created_at": _isoformat_z(generated),
            "read_only": True,
            "no_files_modified_except_operator_brief_artifacts": True,
        },
    }
    _assert_operator_brief_safety_invariants(payload)
    return payload


def render_daily_trading_system_operator_brief_markdown(payload: dict[str, Any]) -> str:
    brief_status = _string_value(payload.get("brief_status")) or BRIEF_ERROR
    snapshot = _mapping(payload.get("system_snapshot"))
    governance = _mapping(payload.get("parameter_governance"))
    pipeline = _mapping(payload.get("pipeline_health"))
    freshness = _mapping(payload.get("data_freshness"))
    market_report = _mapping(payload.get("market_report_status"))
    weight_iteration = _mapping(payload.get("weight_iteration_status"))
    pending = _mapping(payload.get("pending_manual_actions"))
    alerts = _mapping(payload.get("alerts"))
    links = _mapping(payload.get("links"))

    lines = [f"# Daily Trading System Operator Brief - {payload.get('date')}", ""]
    if brief_status == BRIEF_URGENT:
        lines.extend(["## URGENT: Manual Attention Required", ""])
    elif brief_status == BRIEF_ACTION_REQUIRED:
        lines.extend(["## Action Required", ""])
    elif brief_status == BRIEF_WATCH:
        lines.extend(["## Watch: Monitoring Recommended", ""])
    elif brief_status == BRIEF_SAFETY_BLOCKED:
        lines.extend(["## Operator Brief Safety Blocked", ""])
        lines.extend(
            [
                (
                    "Operator brief generation was safety-blocked because input "
                    "artifact safety fields are invalid."
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## 1. Executive Summary",
            "",
            f"- Brief Status: `{brief_status}`",
            f"- Summary Level: `{payload.get('summary_level', SUMMARY_UNKNOWN)}`",
            (
                "- Can Trust Outputs Today: "
                f"`{_bool_text(snapshot.get('can_trust_outputs_today'))}`"
            ),
            (
                "- Manual Action Required: "
                f"`{_bool_text(snapshot.get('manual_action_required'))}`"
            ),
            f"- Pipeline Health: `{pipeline.get('status', STATUS_UNKNOWN)}`",
            f"- Data Freshness: `{freshness.get('status', STATUS_UNKNOWN)}`",
            f"- Headline: {payload.get('headline') or ''}",
            "",
            "## 2. Parameter Governance",
            "",
            f"- Digest Status: `{governance.get('digest_status', 'MISSING')}`",
            f"- Governance State: `{governance.get('governance_state', 'MISSING')}`",
            f"- Action Required: `{_bool_text(governance.get('action_required'))}`",
            f"- Action Level: `{governance.get('action_level', 'NONE')}`",
            f"- Headline: {governance.get('headline') or ''}",
            "",
            "## 3. Pipeline Health",
            "",
            f"- Status: `{pipeline.get('status', STATUS_UNKNOWN)}`",
            f"- Summary Level: `{pipeline.get('summary_level', SUMMARY_UNKNOWN)}`",
            f"- Required Pipelines: `{pipeline.get('required_pipelines', 0)}`",
            ("- Missing Required Pipelines: " f"`{pipeline.get('missing_required_pipelines', 0)}`"),
            ("- Stale Required Pipelines: " f"`{pipeline.get('stale_required_pipelines', 0)}`"),
            f"- Critical Pipelines: `{pipeline.get('critical_pipelines', 0)}`",
            f"- Warning Pipelines: `{pipeline.get('warning_pipelines', 0)}`",
            f"- Report: `{pipeline.get('markdown_path') or ''}`",
            f"- Headline: {pipeline.get('headline') or ''}",
            "- Notes:",
        ]
    )
    lines.extend(_markdown_bullets(_strings(pipeline.get("notes"))))
    lines.extend(
        [
            "",
            "## 4. Data Freshness",
            "",
            f"- Status: `{freshness.get('status', STATUS_UNKNOWN)}`",
            f"- Summary Level: `{freshness.get('summary_level', SUMMARY_UNKNOWN)}`",
            f"- Required Sources: `{freshness.get('required_sources', 0)}`",
            ("- Missing Required Sources: " f"`{freshness.get('missing_required_sources', 0)}`"),
            ("- Stale Required Sources: " f"`{freshness.get('stale_required_sources', 0)}`"),
            f"- Critical Sources: `{freshness.get('critical_sources', 0)}`",
            f"- Warning Sources: `{freshness.get('warning_sources', 0)}`",
            f"- Report: `{freshness.get('markdown_path') or ''}`",
            f"- Headline: {freshness.get('headline') or ''}",
            "- Notes:",
        ]
    )
    lines.extend(_markdown_bullets(_strings(freshness.get("notes"))))
    lines.extend(
        [
            "",
            "## 5. Market Report Status",
            "",
            f"- Status: `{market_report.get('status', STATUS_UNKNOWN)}`",
            f"- Latest Report: `{market_report.get('latest_report_path') or ''}`",
            "- Notes:",
        ]
    )
    lines.extend(_markdown_bullets(_strings(market_report.get("notes"))))
    lines.extend(
        [
            "",
            "## 6. Weight Iteration / Governance Artifacts",
            "",
            (
                "- Shadow Iteration: "
                f"`{weight_iteration.get('latest_shadow_iteration_status', STATUS_UNKNOWN)}`"
            ),
            (
                "- Shadow vs Production Comparison: "
                f"`{weight_iteration.get('latest_comparison_status', STATUS_UNKNOWN)}`"
            ),
            (
                "- Multi-day Review: "
                f"`{weight_iteration.get('latest_multi_day_review_status', STATUS_UNKNOWN)}`"
            ),
            (
                "- Promotion Lifecycle Audit: "
                f"`{weight_iteration.get('latest_lifecycle_audit_status', STATUS_UNKNOWN)}`"
            ),
            "",
            "## 7. Pending Manual Actions",
            "",
            "| Action | Source | Priority | Reason |",
            "|---|---|---:|---|",
        ]
    )
    pending_items = _records(pending.get("items"))
    if not pending_items:
        lines.append("| None | - | - | - |")
    else:
        for item in pending_items:
            lines.append(
                "| "
                f"{_table_text(item.get('action'))} | "
                f"{_table_text(item.get('source'))} | "
                f"{_table_text(item.get('priority'))} | "
                f"{_table_text(item.get('reason'))} |"
            )

    lines.extend(["", "## 8. Alerts", "", "### Critical", ""])
    lines.extend(_markdown_bullets(_strings(alerts.get("critical"))))
    lines.extend(["", "### Warnings", ""])
    lines.extend(_markdown_bullets(_strings(alerts.get("warnings"))))
    lines.extend(["", "### Notes", ""])
    lines.extend(_markdown_bullets(_strings(alerts.get("notes"))))
    lines.extend(["", "## 9. Recommended Next Steps", ""])
    lines.extend(
        [f"- {item}" for item in _strings(payload.get("recommended_next_steps"))]
        or ["- Review the operator brief JSON."]
    )
    lines.extend(
        [
            "",
            "## 10. Links",
            "",
            (
                "- Parameter Governance Digest: "
                f"`{links.get('parameter_governance_digest_markdown', '')}`"
            ),
            (
                "- Parameter Governance Web View: "
                f"`{links.get('parameter_governance_web_view', '')}`"
            ),
            f"- Operator Brief JSON: `{links.get('operator_brief_json', '')}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_daily_trading_system_operator_brief_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Daily Trading System Operator Brief Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- brief_status: `{payload.get('brief_status')}`",
            f"- summary_level: `{payload.get('summary_level')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- operator_brief_only: `true`",
            "- read_only: `true`",
            "- apply_executed_by_operator_brief: `false`",
            "- rollback_executed_by_operator_brief: `false`",
            "- broker_execution: `false`",
            "- replay_execution: `false`",
            "- trading_execution: `false`",
            f"- operator_brief_json: `{payload.get('operator_brief_json')}`",
            f"- operator_brief_markdown: `{payload.get('operator_brief_markdown')}`",
            "",
        ]
    )


def _resolve_digest_path(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
    lookback_days: int,
) -> Path:
    if explicit_path is not None:
        return explicit_path
    return _latest_dated_artifact(
        roots=(default_parameter_governance_digest_root(data_root),),
        prefix="parameter_governance_daily_digest_",
        suffixes=(".json",),
        as_of=as_of,
        lookback_days=lookback_days,
        default_path=default_parameter_governance_digest_json_path(data_root, as_of),
    )


def _resolve_pipeline_health_summary_path(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
    lookback_days: int,
) -> Path:
    if explicit_path is not None:
        return explicit_path
    return _latest_dated_artifact(
        roots=(default_pipeline_health_summary_root(data_root),),
        prefix="pipeline_health_summary_",
        suffixes=(".json",),
        as_of=as_of,
        lookback_days=lookback_days,
        default_path=default_pipeline_health_summary_json_path(data_root, as_of),
    )


def _resolve_data_freshness_summary_path(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
    lookback_days: int,
) -> Path:
    if explicit_path is not None:
        return explicit_path
    return _latest_dated_artifact(
        roots=(default_data_freshness_summary_root(data_root),),
        prefix="data_freshness_summary_",
        suffixes=(".json",),
        as_of=as_of,
        lookback_days=lookback_days,
        default_path=default_data_freshness_summary_json_path(data_root, as_of),
    )


def _latest_dated_artifact(
    *,
    roots: tuple[Path, ...],
    prefix: str,
    suffixes: tuple[str, ...],
    as_of: date,
    lookback_days: int,
    default_path: Path,
) -> Path:
    earliest = as_of - timedelta(days=lookback_days - 1)
    candidates: list[tuple[date, int, Path]] = []
    for root in roots:
        if not root.exists():
            continue
        for suffix in suffixes:
            for path in root.glob(f"{prefix}*{suffix}"):
                raw_date = path.name.removeprefix(prefix).removesuffix(suffix)
                parsed = _parse_iso_date(raw_date)
                if parsed is not None and earliest <= parsed <= as_of:
                    priority = 1 if suffix == ".json" else 0
                    candidates.append((parsed, priority, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: (item[0], item[1]))[2]


def _optional_artifact_path(
    *,
    data_root: Path,
    as_of: date,
    lookback_days: int,
    prefixes: tuple[str, ...],
    default_filename: str,
    subdirs: tuple[Path, ...],
    suffixes: tuple[str, ...] = (".json", ".md"),
) -> Path:
    roots = _candidate_roots(data_root, subdirs)
    default_path = data_root.parent / "outputs" / "reports" / default_filename
    earliest = as_of - timedelta(days=lookback_days - 1)
    candidates: list[tuple[date, int, Path]] = []
    for root in roots:
        if not root.exists():
            continue
        for prefix in prefixes:
            for suffix in suffixes:
                for path in root.glob(f"{prefix}*{suffix}"):
                    raw_date = path.name.removeprefix(prefix).removesuffix(suffix)
                    parsed = _parse_iso_date(raw_date)
                    if parsed is not None and earliest <= parsed <= as_of:
                        priority = 1 if suffix == ".json" else 0
                        candidates.append((parsed, priority, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: (item[0], item[1]))[2]


def _candidate_roots(data_root: Path, subdirs: tuple[Path, ...]) -> tuple[Path, ...]:
    roots = [data_root.parent / "outputs" / "reports", data_root]
    roots.extend(subdirs)
    return tuple(dict.fromkeys(roots))


def _load_digest(path: Path) -> tuple[dict[str, Any], str, str]:
    if not path.exists() or not path.is_file():
        return {}, BRIEF_INPUT_MISSING, f"Parameter governance daily digest not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, BRIEF_INPUT_INVALID, f"Parameter governance daily digest JSON is invalid: {exc}"
    except OSError as exc:
        return {}, BRIEF_INPUT_INVALID, f"Parameter governance daily digest cannot be read: {exc}"
    if not isinstance(payload, dict):
        return {}, BRIEF_INPUT_INVALID, "Parameter governance daily digest must be a JSON object."
    if payload.get("task_id") != DIGEST_TASK_ID:
        return (
            payload,
            BRIEF_INPUT_INVALID,
            "Parameter governance daily digest task_id must be TRADING-021.",
        )
    return payload, BRIEF_OK, ""


def _digest_safety_validation(
    digest: dict[str, Any],
    *,
    input_status: str,
    input_reason: str,
) -> dict[str, Any]:
    if input_status != BRIEF_OK:
        return {
            "status": "FAIL",
            "digest_task_id_valid": False,
            "digest_production_effect_none": False,
            "digest_governance_only": False,
            "digest_no_execution_flags": False,
            "operator_brief_no_execution_flags": True,
            "blocking_reasons": [input_reason] if input_reason else [],
        }

    checks = {
        "digest_task_id_valid": digest.get("task_id") == DIGEST_TASK_ID,
        "digest_production_effect_none": digest.get("production_effect") == PRODUCTION_EFFECT_NONE,
        "digest_governance_only": digest.get("digest_only") is True
        and digest.get("governance_only") is True,
        "digest_no_execution_flags": digest.get("apply_executed_by_digest") is False
        and digest.get("rollback_executed_by_digest") is False
        and digest.get("broker_execution") is False
        and digest.get("replay_execution") is False
        and digest.get("trading_execution") is False,
        "operator_brief_no_execution_flags": True,
    }
    blocking_reasons = [
        _safety_reason(field)
        for field, passed in checks.items()
        if not passed and field != "operator_brief_no_execution_flags"
    ]
    return {
        "status": "PASS" if not blocking_reasons else "FAIL",
        **checks,
        "blocking_reasons": blocking_reasons,
    }


def _safety_reason(field: str) -> str:
    return {
        "digest_task_id_valid": "Parameter governance daily digest task_id must be TRADING-021.",
        "digest_production_effect_none": "Digest production_effect must be none.",
        "digest_governance_only": "Digest must keep digest_only=true and governance_only=true.",
        "digest_no_execution_flags": (
            "Digest must not execute apply, rollback, broker, replay, or trading."
        ),
    }.get(field, field)


def _pipeline_health_status(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
    lookback_days: int,
) -> dict[str, Any]:
    path = _resolve_pipeline_health_summary_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=explicit_path,
        lookback_days=lookback_days,
    )
    if not path.exists():
        return {
            "artifact": _optional_artifact_record(path),
            "status": STATUS_UNKNOWN,
            "available": False,
            "health_status": STATUS_UNKNOWN,
            "summary_level": SUMMARY_UNKNOWN,
            "required_pipelines": 0,
            "missing_required_pipelines": 0,
            "stale_required_pipelines": 0,
            "critical_pipelines": 0,
            "warning_pipelines": 0,
            "critical_pipeline_items": [],
            "action_pipeline_items": [],
            "missing_required_pipeline_items": [],
            "stale_pipeline_items": [],
            "headline": "",
            "markdown_path": str(path.with_suffix(".md")),
            "alerts": {"critical": [], "warnings": [], "notes": []},
            "safety_validation": {
                "status": "PASS",
                "pipeline_health_task_id_valid": True,
                "pipeline_health_production_effect_none": True,
                "pipeline_health_only": True,
                "pipeline_health_read_only": True,
                "pipeline_health_no_pipeline_execution": True,
                "pipeline_health_no_broker_execution": True,
                "pipeline_health_no_replay_execution": True,
                "pipeline_health_no_trading_execution": True,
                "blocking_reasons": [],
            },
            "notes": ["No TRADING-023 pipeline health summary artifact was found."],
        }
    payload, read_error = _load_json_object_with_error(path)
    if read_error:
        return _invalid_pipeline_health_status(path, read_error)
    safety = _pipeline_health_summary_safety_validation(payload)
    coverage = _mapping(payload.get("coverage"))
    health_status = _string_value(payload.get("health_status")) or STATUS_UNKNOWN
    output_artifacts = _mapping(payload.get("output_artifacts"))
    markdown_path = _string_value(_mapping(output_artifacts.get("markdown")).get("path"))
    if not markdown_path:
        markdown_path = str(path.with_suffix(".md"))
    critical_items = _records(payload.get("critical_pipelines"))
    missing_items = _records(payload.get("missing_required_pipelines"))
    stale_items = _records(payload.get("stale_pipelines"))
    action_items = [
        item
        for item in _records(payload.get("pipeline_results"))
        if item.get("required") is True
        and _string_value(item.get("status")) in {"ACTION_REQUIRED", "STALE"}
    ]
    return {
        "artifact": _optional_artifact_record(path),
        "status": health_status,
        "available": True,
        "health_status": health_status,
        "summary_level": _string_value(payload.get("summary_level")) or SUMMARY_UNKNOWN,
        "required_pipelines": int(coverage.get("required_pipelines") or 0),
        "missing_required_pipelines": int(coverage.get("missing_required_pipelines") or 0),
        "stale_required_pipelines": int(coverage.get("stale_required_pipelines") or 0),
        "critical_pipelines": int(coverage.get("critical_pipelines") or 0),
        "warning_pipelines": int(coverage.get("warning_pipelines") or 0),
        "critical_pipeline_items": critical_items,
        "action_pipeline_items": action_items,
        "missing_required_pipeline_items": missing_items,
        "stale_pipeline_items": stale_items,
        "headline": _string_value(payload.get("headline")),
        "markdown_path": markdown_path,
        "alerts": _mapping(payload.get("alerts")),
        "safety_validation": safety,
        "notes": _strings(_mapping(payload.get("operator_brief_integration")).get("notes")),
    }


def _data_freshness_status(
    *,
    as_of: date,
    data_root: Path,
    explicit_path: Path | None,
    lookback_days: int,
) -> dict[str, Any]:
    path = _resolve_data_freshness_summary_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=explicit_path,
        lookback_days=lookback_days,
    )
    if not path.exists():
        return {
            "artifact": _optional_artifact_record(path),
            "status": STATUS_UNKNOWN,
            "available": False,
            "freshness_status": STATUS_UNKNOWN,
            "summary_level": SUMMARY_UNKNOWN,
            "required_sources": 0,
            "missing_required_sources": 0,
            "stale_required_sources": 0,
            "critical_sources": 0,
            "warning_sources": 0,
            "critical_source_items": [],
            "stale_source_items": [],
            "missing_required_source_items": [],
            "headline": "",
            "markdown_path": str(path.with_suffix(".md")),
            "alerts": {"critical": [], "warnings": [], "notes": []},
            "safety_validation": {
                "status": "PASS",
                "data_freshness_task_id_valid": True,
                "data_freshness_production_effect_none": True,
                "data_freshness_only": True,
                "data_freshness_read_only": True,
                "data_freshness_no_data_download": True,
                "data_freshness_no_pipeline_execution": True,
                "data_freshness_no_broker_execution": True,
                "data_freshness_no_replay_execution": True,
                "data_freshness_no_trading_execution": True,
                "blocking_reasons": [],
            },
            "notes": ["No TRADING-024 data freshness summary artifact was found."],
        }
    payload, read_error = _load_json_object_with_error(path)
    if read_error:
        return _invalid_data_freshness_status(path, read_error)
    safety = _data_freshness_summary_safety_validation(payload)
    coverage = _mapping(payload.get("coverage"))
    freshness_status = _string_value(payload.get("freshness_status")) or STATUS_UNKNOWN
    output_artifacts = _mapping(payload.get("output_artifacts"))
    markdown_path = _string_value(_mapping(output_artifacts.get("markdown")).get("path"))
    if not markdown_path:
        markdown_path = str(path.with_suffix(".md"))
    return {
        "artifact": _optional_artifact_record(path),
        "status": freshness_status,
        "available": True,
        "freshness_status": freshness_status,
        "summary_level": _string_value(payload.get("summary_level")) or SUMMARY_UNKNOWN,
        "required_sources": int(coverage.get("required_sources") or 0),
        "missing_required_sources": int(coverage.get("missing_required_sources") or 0),
        "stale_required_sources": int(coverage.get("stale_required_sources") or 0),
        "critical_sources": int(coverage.get("critical_sources") or 0),
        "warning_sources": int(coverage.get("warning_sources") or 0),
        "critical_source_items": _records(payload.get("critical_sources")),
        "stale_source_items": _records(payload.get("stale_sources")),
        "missing_required_source_items": _records(payload.get("missing_required_sources")),
        "headline": _string_value(payload.get("headline")),
        "markdown_path": markdown_path,
        "alerts": _mapping(payload.get("alerts")),
        "safety_validation": safety,
        "notes": _strings(_mapping(payload.get("operator_brief_integration")).get("notes")),
    }


def _invalid_pipeline_health_status(path: Path, reason: str) -> dict[str, Any]:
    return {
        "artifact": _optional_artifact_record(path),
        "status": STATUS_UNKNOWN,
        "available": True,
        "health_status": STATUS_UNKNOWN,
        "summary_level": SUMMARY_UNKNOWN,
        "required_pipelines": 0,
        "missing_required_pipelines": 0,
        "stale_required_pipelines": 0,
        "critical_pipelines": 0,
        "warning_pipelines": 0,
        "critical_pipeline_items": [],
        "action_pipeline_items": [],
        "missing_required_pipeline_items": [],
        "stale_pipeline_items": [],
        "headline": "",
        "markdown_path": str(path.with_suffix(".md")),
        "alerts": {"critical": [], "warnings": [], "notes": []},
        "safety_validation": {
            "status": "FAIL",
            "pipeline_health_task_id_valid": False,
            "pipeline_health_production_effect_none": False,
            "pipeline_health_only": False,
            "pipeline_health_read_only": False,
            "pipeline_health_no_pipeline_execution": False,
            "pipeline_health_no_broker_execution": False,
            "pipeline_health_no_replay_execution": False,
            "pipeline_health_no_trading_execution": False,
            "blocking_reasons": [reason],
        },
        "notes": [reason],
    }


def _invalid_data_freshness_status(path: Path, reason: str) -> dict[str, Any]:
    return {
        "artifact": _optional_artifact_record(path),
        "status": STATUS_UNKNOWN,
        "available": True,
        "freshness_status": STATUS_UNKNOWN,
        "summary_level": SUMMARY_UNKNOWN,
        "required_sources": 0,
        "missing_required_sources": 0,
        "stale_required_sources": 0,
        "critical_sources": 0,
        "warning_sources": 0,
        "critical_source_items": [],
        "stale_source_items": [],
        "missing_required_source_items": [],
        "headline": "",
        "markdown_path": str(path.with_suffix(".md")),
        "alerts": {"critical": [], "warnings": [], "notes": []},
        "safety_validation": {
            "status": "FAIL",
            "data_freshness_task_id_valid": False,
            "data_freshness_production_effect_none": False,
            "data_freshness_only": False,
            "data_freshness_read_only": False,
            "data_freshness_no_data_download": False,
            "data_freshness_no_pipeline_execution": False,
            "data_freshness_no_broker_execution": False,
            "data_freshness_no_replay_execution": False,
            "data_freshness_no_trading_execution": False,
            "blocking_reasons": [reason],
        },
        "notes": [reason],
    }


def _pipeline_health_summary_safety_validation(payload: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "pipeline_health_task_id_valid": payload.get("task_id") == PIPELINE_HEALTH_TASK_ID,
        "pipeline_health_production_effect_none": (
            payload.get("production_effect") == PRODUCTION_EFFECT_NONE
        ),
        "pipeline_health_only": payload.get("pipeline_health_only") is True,
        "pipeline_health_read_only": payload.get("read_only") is True,
        "pipeline_health_no_pipeline_execution": (
            payload.get("pipelines_executed_by_health_check") is False
        ),
        "pipeline_health_no_broker_execution": payload.get("broker_execution") is False,
        "pipeline_health_no_replay_execution": payload.get("replay_execution") is False,
        "pipeline_health_no_trading_execution": payload.get("trading_execution") is False,
    }
    blocking_reasons = [
        _summary_safety_reason(field) for field, passed in checks.items() if not passed
    ]
    return {
        "status": "PASS" if not blocking_reasons else "FAIL",
        **checks,
        "blocking_reasons": blocking_reasons,
    }


def _data_freshness_summary_safety_validation(payload: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "data_freshness_task_id_valid": payload.get("task_id") == DATA_FRESHNESS_TASK_ID,
        "data_freshness_production_effect_none": (
            payload.get("production_effect") == PRODUCTION_EFFECT_NONE
        ),
        "data_freshness_only": payload.get("data_freshness_only") is True,
        "data_freshness_read_only": payload.get("read_only") is True,
        "data_freshness_no_data_download": (
            payload.get("data_downloaded_by_freshness_check") is False
        ),
        "data_freshness_no_pipeline_execution": (
            payload.get("pipelines_executed_by_freshness_check") is False
        ),
        "data_freshness_no_broker_execution": payload.get("broker_execution") is False,
        "data_freshness_no_replay_execution": payload.get("replay_execution") is False,
        "data_freshness_no_trading_execution": payload.get("trading_execution") is False,
    }
    blocking_reasons = [
        _summary_safety_reason(field) for field, passed in checks.items() if not passed
    ]
    return {
        "status": "PASS" if not blocking_reasons else "FAIL",
        **checks,
        "blocking_reasons": blocking_reasons,
    }


def _summary_safety_reason(field: str) -> str:
    return {
        "pipeline_health_task_id_valid": ("Pipeline health summary task_id must be TRADING-023."),
        "pipeline_health_production_effect_none": (
            "Pipeline health summary production_effect must be none."
        ),
        "pipeline_health_only": "Pipeline health summary must keep pipeline_health_only=true.",
        "pipeline_health_read_only": "Pipeline health summary must keep read_only=true.",
        "pipeline_health_no_pipeline_execution": (
            "Pipeline health summary must not run pipelines."
        ),
        "pipeline_health_no_broker_execution": (
            "Pipeline health summary must keep broker_execution=false."
        ),
        "pipeline_health_no_replay_execution": (
            "Pipeline health summary must keep replay_execution=false."
        ),
        "pipeline_health_no_trading_execution": (
            "Pipeline health summary must keep trading_execution=false."
        ),
        "data_freshness_task_id_valid": ("Data freshness summary task_id must be TRADING-024."),
        "data_freshness_production_effect_none": (
            "Data freshness summary production_effect must be none."
        ),
        "data_freshness_only": "Data freshness summary must keep data_freshness_only=true.",
        "data_freshness_read_only": "Data freshness summary must keep read_only=true.",
        "data_freshness_no_data_download": (
            "Data freshness summary must not download or refresh data."
        ),
        "data_freshness_no_pipeline_execution": ("Data freshness summary must not run pipelines."),
        "data_freshness_no_broker_execution": (
            "Data freshness summary must keep broker_execution=false."
        ),
        "data_freshness_no_replay_execution": (
            "Data freshness summary must keep replay_execution=false."
        ),
        "data_freshness_no_trading_execution": (
            "Data freshness summary must keep trading_execution=false."
        ),
    }.get(field, field)


def _input_safety_validation(
    *,
    digest_validation: dict[str, Any],
    pipeline_health: dict[str, Any],
    data_freshness: dict[str, Any],
) -> dict[str, Any]:
    pipeline_validation = _mapping(pipeline_health.get("safety_validation"))
    freshness_validation = _mapping(data_freshness.get("safety_validation"))
    blocking_reasons = []
    blocking_reasons.extend(_strings(digest_validation.get("blocking_reasons")))
    blocking_reasons.extend(_strings(pipeline_validation.get("blocking_reasons")))
    blocking_reasons.extend(_strings(freshness_validation.get("blocking_reasons")))
    return {
        **digest_validation,
        "status": "PASS" if not blocking_reasons else "FAIL",
        "pipeline_health": pipeline_validation,
        "data_freshness": freshness_validation,
        "blocking_reasons": list(dict.fromkeys(blocking_reasons)),
    }


def _market_report_status(*, as_of: date, data_root: Path, lookback_days: int) -> dict[str, Any]:
    path = _optional_artifact_path(
        data_root=data_root,
        as_of=as_of,
        lookback_days=lookback_days,
        prefixes=("market_report_", "daily_score_"),
        default_filename=f"market_report_{as_of.isoformat()}.json",
        subdirs=(data_root / "derived" / "market_reports",),
    )
    if not path.exists():
        return {
            "artifact": _optional_artifact_record(path),
            "status": STATUS_UNKNOWN,
            "available": False,
            "latest_report_path": None,
            "latest_report_date": None,
            "notes": ["No market report artifact was found."],
        }
    payload = _read_json_object(path)
    status = _artifact_status(path, payload, default=STATUS_AVAILABLE)
    latest_date = _date_from_artifact_name(path) or as_of
    latest_path = _string_value(payload.get("latest_report_path")) or str(path)
    return {
        "artifact": _optional_artifact_record(path),
        "status": status,
        "available": True,
        "latest_report_path": latest_path,
        "latest_report_date": latest_date.isoformat(),
        "notes": _strings(payload.get("notes")) or [f"Market report artifact found: {path}"],
    }


def _weight_iteration_status(
    *,
    as_of: date,
    data_root: Path,
    lookback_days: int,
    include_optional_artifacts: bool,
) -> dict[str, Any]:
    _ = include_optional_artifacts
    shadow = _latest_existing(
        data_root / "derived" / "weight_iterations" / "shadow" / "candidates",
        "shadow_weight_candidate_",
        as_of,
        lookback_days,
    )
    comparison = _latest_existing(
        data_root / "derived" / "weight_iterations" / "comparison",
        "daily_shadow_vs_production_",
        as_of,
        lookback_days,
    )
    review = _latest_existing(
        data_root / "derived" / "weight_iterations" / "comparison" / "reviews",
        "shadow_vs_production_review_",
        as_of,
        lookback_days,
    )
    audit = _latest_existing(
        data_root / "derived" / "weight_iterations" / "promotion" / "audit",
        "shadow_promotion_lifecycle_audit_",
        as_of,
        lookback_days,
    )
    paths = {
        "latest_shadow_iteration_path": shadow,
        "latest_comparison_path": comparison,
        "latest_multi_day_review_path": review,
        "latest_lifecycle_audit_path": audit,
    }
    any_available = any(path is not None for path in paths.values())
    return {
        "status": STATUS_AVAILABLE if any_available else STATUS_UNKNOWN,
        "latest_shadow_iteration_status": (
            STATUS_AVAILABLE if shadow is not None else STATUS_UNKNOWN
        ),
        "latest_comparison_status": STATUS_AVAILABLE if comparison is not None else STATUS_UNKNOWN,
        "latest_multi_day_review_status": (
            STATUS_AVAILABLE if review is not None else STATUS_UNKNOWN
        ),
        "latest_lifecycle_audit_status": STATUS_AVAILABLE if audit is not None else STATUS_UNKNOWN,
        **{key: None if path is None else str(path) for key, path in paths.items()},
        "notes": (
            [] if any_available else ["No weight iteration or governance artifacts were found."]
        ),
    }


def _latest_existing(
    root: Path,
    prefix: str,
    as_of: date,
    lookback_days: int,
) -> Path | None:
    if not root.exists():
        return None
    earliest = as_of - timedelta(days=lookback_days - 1)
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"{prefix}*.json"):
        raw_date = path.stem.removeprefix(prefix)
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and earliest <= parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _artifact_status(path: Path, payload: dict[str, Any], *, default: str) -> str:
    if payload:
        for key in (
            "status",
            "health_status",
            "data_freshness_status",
            "report_status",
            "pipeline_status",
            "quality_status",
            "evaluation_status",
        ):
            status = _string_value(payload.get(key))
            if status:
                return status
    markdown_status = _read_markdown_status(path)
    return markdown_status or default


def _read_markdown_status(path: Path) -> str:
    if path.suffix.lower() not in {".md", ".html"}:
        return ""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""
    patterns = (
        r"^-\s*状态[：:]\s*`?([^`\n]+)`?",
        r"^-\s*Status[：:]\s*`?([^`\n]+)`?",
        r"^-\s*Pipeline health[：:]\s*`?([^`\n]+)`?",
    )
    for line in text.splitlines():
        stripped = line.strip()
        for pattern in patterns:
            match = re.match(pattern, stripped, flags=re.IGNORECASE)
            if match:
                return match.group(1).strip()
    return ""


def _alerts(
    *,
    digest: dict[str, Any],
    input_status: str,
    input_reason: str,
    safety_validation: dict[str, Any],
    pipeline_health: dict[str, Any],
    data_freshness: dict[str, Any],
) -> dict[str, list[str]]:
    raw_alerts = _mapping(digest.get("alerts"))
    critical = _prefixed_alerts("TRADING-021", _strings(raw_alerts.get("critical")))
    warnings = _prefixed_alerts("TRADING-021", _strings(raw_alerts.get("warnings")))
    notes = _prefixed_alerts("TRADING-021", _strings(raw_alerts.get("notes")))
    digest_status = _string_value(digest.get("digest_status"))
    pipeline_alerts = _mapping(pipeline_health.get("alerts"))
    freshness_alerts = _mapping(data_freshness.get("alerts"))
    pipeline_status = _string_value(pipeline_health.get("health_status")) or _string_value(
        pipeline_health.get("status")
    )
    freshness_status = _string_value(data_freshness.get("freshness_status")) or _string_value(
        data_freshness.get("status")
    )

    if input_status == BRIEF_INPUT_MISSING and input_reason:
        warnings.append(f"[TRADING-022] {input_reason}")
    if input_status == BRIEF_INPUT_INVALID and input_reason:
        critical.append(f"[TRADING-022] {input_reason}")
    if safety_validation.get("status") == "FAIL" and input_status == BRIEF_OK:
        critical.append(
            "[TRADING-022] Operator brief generation was safety-blocked because input artifact "
            "safety fields are invalid."
        )
        critical.extend(
            _prefixed_alerts("TRADING-022", _strings(safety_validation.get("blocking_reasons")))
        )
    critical.extend(_prefixed_alerts("TRADING-023", _strings(pipeline_alerts.get("critical"))))
    warnings.extend(_prefixed_alerts("TRADING-023", _strings(pipeline_alerts.get("warnings"))))
    notes.extend(_prefixed_alerts("TRADING-023", _strings(pipeline_alerts.get("notes"))))
    critical.extend(_prefixed_alerts("TRADING-024", _strings(freshness_alerts.get("critical"))))
    warnings.extend(_prefixed_alerts("TRADING-024", _strings(freshness_alerts.get("warnings"))))
    notes.extend(_prefixed_alerts("TRADING-024", _strings(freshness_alerts.get("notes"))))
    if pipeline_status == "CRITICAL":
        critical.append("[TRADING-023] Pipeline health summary status is CRITICAL.")
    elif pipeline_status in {"ACTION_REQUIRED", "INCOMPLETE", "WATCH", STATUS_UNKNOWN}:
        warnings.append(f"[TRADING-023] Pipeline health summary status is {pipeline_status}.")
    if freshness_status == "CRITICAL":
        critical.append("[TRADING-024] Data freshness summary status is CRITICAL.")
    elif freshness_status in {"STALE", "MISSING", "WATCH", STATUS_UNKNOWN}:
        warnings.append(f"[TRADING-024] Data freshness summary status is {freshness_status}.")
    if digest_status == BRIEF_OK:
        notes.append("[TRADING-021] Parameter governance digest is OK.")
    elif digest_status:
        notes.append(f"[TRADING-021] Parameter governance digest status is {digest_status}.")
    return {
        "critical": list(dict.fromkeys(critical)),
        "warnings": list(dict.fromkeys(warnings)),
        "notes": list(dict.fromkeys(notes)),
    }


def _parameter_governance_status(
    *,
    digest: dict[str, Any],
    input_status: str,
    safety_validation: dict[str, Any],
) -> dict[str, Any]:
    snapshot = _mapping(digest.get("governance_snapshot"))
    digest_status = _string_value(digest.get("digest_status")) or (
        BRIEF_SAFETY_BLOCKED
        if safety_validation.get("status") == "FAIL" and input_status == BRIEF_OK
        else input_status
    )
    return {
        "status": digest_status if input_status == BRIEF_OK else input_status,
        "digest_status": digest_status,
        "summary_level": _string_value(digest.get("summary_level")) or SUMMARY_UNKNOWN,
        "governance_state": _string_value(snapshot.get("governance_state")) or "MISSING",
        "action_required": snapshot.get("action_required") is True,
        "action_level": _string_value(snapshot.get("action_level")) or "NONE",
        "headline": _string_value(digest.get("headline")),
    }


def _pending_manual_actions(
    *,
    digest: dict[str, Any],
    brief_alerts: dict[str, list[str]],
    input_status: str,
    safety_validation: dict[str, Any],
    pipeline_health: dict[str, Any],
    data_freshness: dict[str, Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    snapshot = _mapping(digest.get("governance_snapshot"))
    pending = _mapping(digest.get("pending_items"))

    if input_status == BRIEF_INPUT_MISSING:
        items.append(
            _manual_action(
                "Locate or regenerate TRADING-021 digest",
                "TRADING-022",
                "HIGH",
                "Required parameter governance daily digest is missing.",
            )
        )
    elif input_status == BRIEF_INPUT_INVALID:
        items.append(
            _manual_action(
                "Inspect invalid TRADING-021 digest",
                "TRADING-022",
                "HIGH",
                "Required digest could not be parsed or has the wrong task_id.",
            )
        )
    elif safety_validation.get("status") == "FAIL":
        items.append(
            _manual_action(
                "Inspect input artifact safety fields",
                "TRADING-022",
                "HIGH",
                "One or more input artifact safety validations failed.",
            )
        )

    if snapshot.get("action_required") is True:
        items.append(
            _manual_action(
                "Review parameter governance action",
                "TRADING-021",
                "MEDIUM",
                _string_value(snapshot.get("recommended_action"))
                or "Digest reports action_required=true.",
            )
        )
    if pending.get("pending_apply") is True:
        items.append(
            _manual_action(
                "Review pending apply",
                "TRADING-021",
                "HIGH",
                "pending_apply=true; explicit approved apply remains outside operator brief.",
            )
        )
    if pending.get("pending_rollback") is True:
        items.append(
            _manual_action(
                "Review pending rollback",
                "TRADING-021",
                "HIGH",
                "pending_rollback=true; explicit approved rollback remains outside operator brief.",
            )
        )
    if pending.get("pending_proposal_review") is True:
        items.append(
            _manual_action(
                "Review pending promotion proposal",
                "TRADING-021",
                "MEDIUM",
                "pending_proposal_review=true.",
            )
        )
    for item in _records(pipeline_health.get("critical_pipeline_items")):
        items.append(
            _manual_action(
                "Review critical pipeline health finding",
                "TRADING-023",
                "HIGH",
                _pipeline_issue_reason(item),
            )
        )
    for item in _records(pipeline_health.get("action_pipeline_items")):
        items.append(
            _manual_action(
                "Review required pipeline issue",
                "TRADING-023",
                "MEDIUM",
                _pipeline_issue_reason(item),
            )
        )
    for item in _records(pipeline_health.get("missing_required_pipeline_items")):
        items.append(
            _manual_action(
                "Review missing required pipeline artifact",
                "TRADING-023",
                "MEDIUM",
                _pipeline_issue_reason(item),
            )
        )
    for item in _records(data_freshness.get("critical_source_items")):
        items.append(
            _manual_action(
                "Review critical data freshness finding",
                "TRADING-024",
                "HIGH",
                _source_issue_reason(item),
            )
        )
    for item in _records(data_freshness.get("stale_source_items")):
        items.append(
            _manual_action(
                "Review stale required data source",
                "TRADING-024",
                "MEDIUM",
                _source_issue_reason(item),
            )
        )
    for item in _records(data_freshness.get("missing_required_source_items")):
        items.append(
            _manual_action(
                "Review missing required data source",
                "TRADING-024",
                "MEDIUM",
                _source_issue_reason(item),
            )
        )
    if _strings(brief_alerts.get("critical")):
        items.append(
            _manual_action(
                "Urgent review of critical findings",
                "TRADING-022",
                "HIGH",
                "Critical alerts are present.",
            )
        )
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        unique[(str(item["action"]), str(item["source"]))] = item
    deduped = list(unique.values())
    return {
        "has_pending_actions": bool(deduped),
        "items": deduped,
    }


def _manual_action(action: str, source: str, priority: str, reason: str) -> dict[str, Any]:
    return {
        "action": action,
        "source": source,
        "priority": priority,
        "reason": reason,
    }


def _brief_status(
    *,
    digest: dict[str, Any],
    input_status: str,
    safety_validation: dict[str, Any],
    pipeline_health: dict[str, Any],
    data_freshness: dict[str, Any],
    alerts: dict[str, list[str]],
    pending_manual_actions: dict[str, Any],
) -> str:
    if input_status == BRIEF_INPUT_MISSING:
        return BRIEF_INPUT_MISSING
    if input_status == BRIEF_INPUT_INVALID:
        return BRIEF_INPUT_INVALID
    if safety_validation.get("status") == "FAIL":
        return BRIEF_SAFETY_BLOCKED
    digest_status = _string_value(digest.get("digest_status"))
    pipeline_status = _string_value(pipeline_health.get("health_status")) or _string_value(
        pipeline_health.get("status")
    )
    freshness_status = _string_value(data_freshness.get("freshness_status")) or _string_value(
        data_freshness.get("status")
    )
    if _strings(alerts.get("critical")):
        return BRIEF_URGENT
    if digest_status == BRIEF_URGENT:
        return BRIEF_URGENT
    if pipeline_status == "CRITICAL" or freshness_status == "CRITICAL":
        return BRIEF_URGENT
    if digest_status == BRIEF_ACTION_REQUIRED:
        return BRIEF_ACTION_REQUIRED
    if pipeline_status in {"ACTION_REQUIRED", "INCOMPLETE"}:
        return BRIEF_ACTION_REQUIRED
    if freshness_status in {"STALE", "MISSING"}:
        return BRIEF_ACTION_REQUIRED
    if pending_manual_actions.get("has_pending_actions") is True:
        return BRIEF_ACTION_REQUIRED
    if digest_status == BRIEF_WATCH:
        return BRIEF_WATCH
    if pipeline_status in {"WATCH", STATUS_UNKNOWN}:
        return BRIEF_WATCH
    if freshness_status in {"WATCH", STATUS_UNKNOWN}:
        return BRIEF_WATCH
    if _strings(alerts.get("warnings")):
        return BRIEF_WATCH
    if digest_status == BRIEF_OK and pipeline_status == BRIEF_OK and freshness_status == BRIEF_OK:
        return BRIEF_OK
    return BRIEF_INPUT_INVALID


def _summary_level(brief_status: str) -> str:
    return {
        BRIEF_OK: SUMMARY_NORMAL,
        BRIEF_WATCH: SUMMARY_WATCH,
        BRIEF_ACTION_REQUIRED: SUMMARY_ACTION,
        BRIEF_URGENT: SUMMARY_URGENT,
        BRIEF_INPUT_MISSING: SUMMARY_UNKNOWN,
        BRIEF_INPUT_INVALID: SUMMARY_UNKNOWN,
        BRIEF_SAFETY_BLOCKED: SUMMARY_UNKNOWN,
        BRIEF_ERROR: SUMMARY_UNKNOWN,
    }.get(brief_status, SUMMARY_UNKNOWN)


def _headline(brief_status: str) -> str:
    return {
        BRIEF_OK: "Trading system status is stable. No immediate manual action is required.",
        BRIEF_WATCH: "Trading system is usable, but some areas require monitoring.",
        BRIEF_ACTION_REQUIRED: (
            "Manual action is required before the next governance or pipeline step."
        ),
        BRIEF_URGENT: "Critical system issue detected. Manual inspection is required immediately.",
        BRIEF_INPUT_MISSING: (
            "Required parameter governance digest is missing. Operator brief cannot determine "
            "system state."
        ),
        BRIEF_INPUT_INVALID: (
            "Required parameter governance digest is invalid. Operator brief cannot determine "
            "system state."
        ),
        BRIEF_SAFETY_BLOCKED: (
            "Operator brief generation was blocked by invalid input artifact safety fields."
        ),
        BRIEF_ERROR: "Daily trading system operator brief generation failed.",
    }.get(brief_status, "Trading system status is unknown.")


def _system_snapshot(
    *,
    brief_status: str,
    alerts: dict[str, list[str]],
    pending_manual_actions: dict[str, Any],
) -> dict[str, Any]:
    return {
        "overall_system_status": brief_status,
        "can_trust_outputs_today": brief_status in {BRIEF_OK, BRIEF_WATCH}
        and not _strings(alerts.get("critical")),
        "manual_action_required": pending_manual_actions.get("has_pending_actions") is True
        or brief_status
        in {
            BRIEF_ACTION_REQUIRED,
            BRIEF_URGENT,
            BRIEF_INPUT_MISSING,
            BRIEF_INPUT_INVALID,
            BRIEF_SAFETY_BLOCKED,
            BRIEF_ERROR,
        },
        "has_critical_alerts": bool(_strings(alerts.get("critical"))),
        "has_warnings": bool(_strings(alerts.get("warnings"))),
    }


def _links(
    *,
    digest: dict[str, Any],
    data_root: Path,
    as_of: date,
    output_json_path: Path,
    output_md_path: Path,
) -> dict[str, str]:
    digest_outputs = _mapping(digest.get("output_artifacts"))
    digest_links = _mapping(digest.get("links"))
    digest_markdown = _string_value(_mapping(digest_outputs.get("markdown")).get("path"))
    if not digest_markdown:
        digest_markdown = _string_value(digest_links.get("daily_digest_markdown"))
    if not digest_markdown:
        digest_markdown = str(default_parameter_governance_digest_json_path(data_root, as_of))
        digest_markdown = str(Path(digest_markdown).with_suffix(".md"))
    return {
        "operator_brief_json": str(output_json_path),
        "operator_brief_markdown": str(output_md_path),
        "parameter_governance_digest_markdown": digest_markdown,
        "parameter_governance_web_view": _string_value(
            digest_links.get("governance_web_view_html")
        ),
    }


def _recommended_next_steps(brief_status: str) -> list[str]:
    return {
        BRIEF_OK: [
            "Continue observation.",
            "Review the parameter governance web view if details are needed.",
        ],
        BRIEF_WATCH: [
            "Continue monitoring.",
            "Review pipeline health and data freshness summaries.",
            "Regenerate TRADING-023 or TRADING-024 if their artifacts are missing or stale.",
        ],
        BRIEF_ACTION_REQUIRED: [
            "Review pending manual actions.",
            (
                "Inspect pipeline health and data freshness summaries before relying on "
                "today's outputs."
            ),
            "Do not apply or rollback without explicit approval artifacts and danger flags.",
        ],
        BRIEF_URGENT: [
            "Stop relying on automated outputs until the issue is reviewed.",
            (
                "Inspect critical alerts from parameter governance, pipeline health, and "
                "data freshness."
            ),
            "Confirm no broker/replay/trading execution occurred unexpectedly.",
        ],
        BRIEF_INPUT_MISSING: [
            "Regenerate or locate the latest TRADING-021 parameter governance daily digest.",
            "Do not infer system health from partial artifacts.",
        ],
        BRIEF_INPUT_INVALID: [
            "Regenerate or locate the latest TRADING-021 parameter governance daily digest.",
            "Do not infer system health from partial artifacts.",
        ],
        BRIEF_SAFETY_BLOCKED: [
            "Inspect input artifact safety fields.",
            "Do not continue with operator brief-based decisions until safety fields are fixed.",
        ],
        BRIEF_ERROR: [
            "Inspect the operator brief run log.",
            "Do not infer system health from partial artifacts.",
        ],
    }.get(brief_status, ["Inspect the operator brief artifact."])


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    output_artifacts = _mapping(payload.get("output_artifacts"))
    operator_json = _string_value(_mapping(output_artifacts.get("json")).get("path"))
    operator_markdown = _string_value(_mapping(output_artifacts.get("markdown")).get("path"))
    alerts = _mapping(payload.get("alerts"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": _isoformat_z(generated_at),
        "run_status": "PASS" if payload.get("brief_status") != BRIEF_ERROR else BRIEF_ERROR,
        "brief_status": payload.get("brief_status"),
        "summary_level": payload.get("summary_level"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "operator_brief_only": True,
        "read_only": True,
        "apply_executed_by_operator_brief": False,
        "rollback_executed_by_operator_brief": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "safe_for_scheduler": True,
        "critical_alert_count": len(_strings(alerts.get("critical"))),
        "warning_count": len(_strings(alerts.get("warnings"))),
        "operator_brief_json": operator_json,
        "operator_brief_markdown": operator_markdown,
    }


def _error_payload(
    *,
    as_of: date,
    data_root: Path,
    parameter_governance_digest_file: Path | None,
    pipeline_health_summary_file: Path | None,
    data_freshness_summary_file: Path | None,
    lookback_days: int,
    include_optional_artifacts: bool,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    digest_path = _resolve_digest_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=parameter_governance_digest_file,
        lookback_days=lookback_days,
    )
    pipeline_path = _resolve_pipeline_health_summary_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=pipeline_health_summary_file,
        lookback_days=lookback_days,
    )
    freshness_path = _resolve_data_freshness_summary_path(
        as_of=as_of,
        data_root=data_root,
        explicit_path=data_freshness_summary_file,
        lookback_days=lookback_days,
    )
    _ = include_optional_artifacts
    alerts = {"critical": [error], "warnings": [], "notes": []}
    pending = {
        "has_pending_actions": True,
        "items": [
            _manual_action(
                "Inspect operator brief run error",
                "TRADING-022",
                "HIGH",
                error,
            )
        ],
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
        "operator_brief_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "apply_executed_by_operator_brief": False,
        "rollback_executed_by_operator_brief": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "brief_status": BRIEF_ERROR,
        "summary_level": SUMMARY_UNKNOWN,
        "headline": _headline(BRIEF_ERROR),
        "input_artifacts": {
            "parameter_governance_daily_digest": _artifact_record(digest_path),
            "pipeline_health_summary": _optional_artifact_record(pipeline_path),
            "data_freshness_summary": _optional_artifact_record(freshness_path),
            "market_report": _optional_artifact_record(
                data_root.parent / "outputs" / "reports" / f"market_report_{as_of}.json"
            ),
        },
        "system_snapshot": _system_snapshot(
            brief_status=BRIEF_ERROR,
            alerts=alerts,
            pending_manual_actions=pending,
        ),
        "parameter_governance": {
            "status": BRIEF_ERROR,
            "digest_status": BRIEF_ERROR,
            "summary_level": SUMMARY_UNKNOWN,
            "governance_state": "ERROR",
            "action_required": True,
            "action_level": "REVIEW_REQUIRED",
            "headline": "",
        },
        "pipeline_health": {
            "status": STATUS_UNKNOWN,
            "available": False,
            "health_status": STATUS_UNKNOWN,
            "summary_level": SUMMARY_UNKNOWN,
            "required_pipelines": 0,
            "missing_required_pipelines": 0,
            "stale_required_pipelines": 0,
            "critical_pipelines": 0,
            "warning_pipelines": 0,
            "markdown_path": str(pipeline_path.with_suffix(".md")),
            "notes": [],
        },
        "data_freshness": {
            "status": STATUS_UNKNOWN,
            "available": False,
            "freshness_status": STATUS_UNKNOWN,
            "summary_level": SUMMARY_UNKNOWN,
            "required_sources": 0,
            "missing_required_sources": 0,
            "stale_required_sources": 0,
            "critical_sources": 0,
            "warning_sources": 0,
            "markdown_path": str(freshness_path.with_suffix(".md")),
            "notes": [],
        },
        "market_report_status": {
            "status": STATUS_UNKNOWN,
            "available": False,
            "latest_report_path": None,
            "latest_report_date": None,
            "notes": [],
        },
        "weight_iteration_status": {
            "status": STATUS_UNKNOWN,
            "latest_shadow_iteration_status": STATUS_UNKNOWN,
            "latest_comparison_status": STATUS_UNKNOWN,
            "latest_multi_day_review_status": STATUS_UNKNOWN,
            "latest_lifecycle_audit_status": STATUS_UNKNOWN,
            "notes": [],
        },
        "pending_manual_actions": pending,
        "alerts": alerts,
        "recommended_next_steps": _recommended_next_steps(BRIEF_ERROR),
        "links": {
            "operator_brief_json": str(output_json_path),
            "operator_brief_markdown": str(output_md_path),
            "parameter_governance_digest_markdown": str(digest_path.with_suffix(".md")),
            "parameter_governance_web_view": "",
        },
        "safety_validation": {
            "status": "FAIL",
            "digest_task_id_valid": False,
            "digest_production_effect_none": False,
            "digest_governance_only": False,
            "digest_no_execution_flags": False,
            "operator_brief_no_execution_flags": True,
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
            "created_by": "scripts/run_daily_trading_system_operator_brief.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_operator_brief_artifacts": True,
        },
    }


def _artifact_record(path: Path) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "status": STATUS_FOUND if exists else "MISSING",
        "path": str(path),
        "sha256": _sha256(path) if exists else None,
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else 0,
    }


def _optional_artifact_record(path: Path) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "status": STATUS_FOUND if exists else OPTIONAL_NOT_FOUND,
        "path": str(path) if exists else None,
        "sha256": _sha256(path) if exists else None,
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else 0,
    }


def _without_artifact(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "artifact"}


def _read_json_object(path: Path) -> dict[str, Any]:
    if path.suffix.lower() != ".json" or not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_json_object_with_error(path: Path) -> tuple[dict[str, Any], str]:
    if path.suffix.lower() != ".json":
        return {}, f"Artifact must be JSON: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, f"Artifact JSON is invalid: {exc}"
    except OSError as exc:
        return {}, f"Artifact cannot be read: {exc}"
    if not isinstance(payload, dict):
        return {}, "Artifact JSON must be an object."
    return payload, ""


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _assert_operator_brief_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("operator brief production_effect must remain none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("operator brief must remain manual_review_only")
    if payload.get("operator_brief_only") is not True:
        raise ValueError("operator brief must remain operator_brief_only")
    if payload.get("read_only") is not True:
        raise ValueError("operator brief must remain read_only")
    if payload.get("apply_executed_by_operator_brief") is not False:
        raise ValueError("operator brief must not execute apply")
    if payload.get("rollback_executed_by_operator_brief") is not False:
        raise ValueError("operator brief must not execute rollback")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"operator brief must keep {field}=false")
    if payload.get("safe_for_scheduler") is not True:
        raise ValueError("operator brief should be scheduler-safe")


def _is_critical_status(status: str) -> bool:
    return status.upper() in {"FAIL", "ERROR", "BLOCKED", "BLOCKED_ENV", "BLOCKED_VISIBILITY"}


def _is_warning_status(status: str) -> bool:
    normalized = status.upper()
    return normalized.startswith("PASS_WITH_") or normalized in {
        "WATCH",
        "ACTIVE_WARNINGS",
        "WARNING",
        "WARN",
        "STALE",
        "LIMITED",
    }


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


def _prefixed_alerts(source: str, items: list[str]) -> list[str]:
    prefix = f"[{source}] "
    return [item if item.startswith(prefix) else f"{prefix}{item}" for item in items]


def _pipeline_issue_reason(item: dict[str, Any]) -> str:
    pipeline_id = _string_value(item.get("pipeline_id")) or "unknown_pipeline"
    reason = _string_value(item.get("reason")) or _string_value(item.get("status"))
    return f"Pipeline {pipeline_id}: {reason or 'requires review'}."


def _source_issue_reason(item: dict[str, Any]) -> str:
    source_id = _string_value(item.get("source_id")) or "unknown_source"
    reason = _string_value(item.get("reason")) or _string_value(item.get("status"))
    return f"Required source {source_id}: {reason or 'requires review'}."


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _date_from_artifact_name(path: Path) -> date | None:
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", path.name)
    parsed = [_parse_iso_date(value) for value in matches]
    dates = [value for value in parsed if value is not None]
    return dates[-1] if dates else None


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bool_text(value: Any) -> str:
    return str(value is True).lower()


def _inline_list(items: list[str]) -> str:
    return "`None`" if not items else ", ".join(f"`{item}`" for item in items)


def _markdown_bullets(items: list[str]) -> list[str]:
    return [f"- {item}" for item in items] or ["- None."]


def _table_text(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")
