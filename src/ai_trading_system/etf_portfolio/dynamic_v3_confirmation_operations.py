from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DEFAULT_CONFIRMATION_EVALUATION_DIR,
    DEFAULT_CONFIRMATION_PROGRESS_DIR,
    DEFAULT_CONFIRMATION_REGISTRY_DIR,
    DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    DEFAULT_RULE_REVIEW_CYCLE_DIR,
    confirmation_evaluation_report_payload,
    confirmation_progress_report_payload,
    rule_review_cycle_report_payload,
    run_confirmation_evaluation,
    run_rule_review_cycle,
    update_confirmation_progress,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _artifact_dir_from_latest,
    _check,
    _float,
    _int,
    _mapping,
    _read_json,
    _read_jsonl,
    _read_optional_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
    _validation_payload,
    _write_json,
    _write_jsonl,
    _write_text,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_CONSENSUS_RISK_DIR,
    DEFAULT_EVIDENCE_TREND_DIR,
    DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
    DEFAULT_LIMITED_VS_NOTRADE_DIR,
    DEFAULT_OUTCOME_DUE_DIR,
    DEFAULT_OUTCOME_UPDATE_DIR,
    DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    run_evidence_trend,
    run_forward_outcome_decision,
    run_outcome_due_scan,
    run_outcome_update,
    run_outcome_update_review,
    run_rolling_evidence_refresh,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_RATES_CACHE_PATH,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH

DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "confirmation_cycle_schedule_v1.yaml"
)
DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "pressure_regime_tagging_v1.yaml"
)
DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_cycle_plan"
)
DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_cycle_weekly"
)
DEFAULT_PRESSURE_REGIME_TAG_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_regime_tag"
DEFAULT_CONFIRMATION_DASHBOARD_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_dashboard"
DEFAULT_RULE_REVIEW_QUEUE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rule_review_queue"

PRESSURE_TAGS = (
    "tech_drawdown",
    "risk_off",
    "semiconductor_pullback",
    "sideways_choppy",
    "strong_recovery",
    "ai_trend",
)
PRESSURE_VALIDATION_TAGS = {"tech_drawdown", "risk_off", "semiconductor_pullback"}
REQUIRED_CONFIRMATION_CYCLE_STEPS = (
    "outcome_due_scan",
    "outcome_update_review",
    "outcome_update_if_ready",
    "rolling_evidence_refresh",
    "confirmation_progress_update",
    "confirmation_evaluate",
    "rule_review_cycle",
    "owner_decision_queue_update",
    "weekly_dashboard",
    "reader_brief_update",
)


class DynamicV3ConfirmationOperationsError(ValueError):
    """Raised when confirmation weekly operations artifacts fail closed."""


def validate_confirmation_cycle_schedule_config(
    *, config_path: Path = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH
) -> dict[str, Any]:
    config = _read_yaml_config(config_path)
    schedule = _mapping(config.get("schedule"))
    safety = _mapping(config.get("safety"))
    execution = _mapping(config.get("execution"))
    steps = [_text(item) for item in config.get("steps", []) if _text(item)]
    checks = [
        _check("config_exists", config_path.exists(), str(config_path)),
        _check("schema_version_supported", _int(config.get("schema_version")) == 1, "schema=1"),
        _check("cadence_weekly", schedule.get("cadence") == "weekly", "cadence=weekly"),
        _check("timezone_present", bool(_text(schedule.get("timezone"))), "timezone"),
        _check(
            "required_steps_present",
            set(steps) >= set(REQUIRED_CONFIRMATION_CYCLE_STEPS),
            "required weekly steps",
        ),
        _check(
            "dry_run_default_enabled",
            execution.get("dry_run_default") is True,
            "weekly runner defaults to dry-run",
        ),
        _check(
            "explicit_update_required",
            execution.get("require_explicit_update") is True,
            "outcome update requires explicit flag",
        ),
        _check(
            "safety_no_broker",
            safety.get("broker_action_allowed") is False
            and safety.get("broker_action_taken") is False,
            "broker action disabled",
        ),
        _check(
            "safety_no_production",
            safety.get("production_effect") == "none"
            and safety.get("auto_apply_policy") is False,
            "no production or auto apply",
        ),
        _check(
            "owner_review_required",
            safety.get("owner_approval_required") is True,
            "owner approval required",
        ),
    ]
    payload = _validation_payload(
        report_type="etf_dynamic_v3_confirmation_cycle_schedule_config_validation",
        artifact_id_key="config_path",
        artifact_id=str(config_path),
        checks=checks,
    )
    payload["config"] = config
    return payload


def build_confirmation_cycle_plan(
    *,
    config_path: Path = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    validation = validate_confirmation_cycle_schedule_config(config_path=config_path)
    if validation["status"] != "PASS":
        raise DynamicV3ConfirmationOperationsError("confirmation cycle config validation failed")
    config = _mapping(validation.get("config"))
    plan_id = _stable_id("confirmation-cycle-plan", str(config_path), generated.isoformat())
    plan_dir = _unique_dir(output_dir / plan_id)
    plan_dir.mkdir(parents=True, exist_ok=False)
    safety = _schedule_safety(config)
    commands = _scheduled_commands(config_path)
    command_pack = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_scheduled_command_pack",
        "plan_id": plan_dir.name,
        "cadence": _text(_mapping(config.get("schedule")).get("cadence"), "weekly"),
        "preferred_weekday": _text(_mapping(config.get("schedule")).get("preferred_weekday")),
        "timezone": _text(_mapping(config.get("schedule")).get("timezone")),
        "safety": safety,
        "commands": commands,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_cycle_plan_manifest",
        "plan_id": plan_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "config_path": str(config_path),
        "planned_step_count": len(commands),
        "confirmation_cycle_plan_manifest_path": str(
            plan_dir / "confirmation_cycle_plan_manifest.json"
        ),
        "scheduled_command_pack_path": str(plan_dir / "scheduled_command_pack.json"),
        "confirmation_cycle_runbook_path": str(plan_dir / "confirmation_cycle_runbook.md"),
        "confirmation_cycle_plan_report_path": str(
            plan_dir / "confirmation_cycle_plan_report.md"
        ),
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    _write_json(plan_dir / "confirmation_cycle_plan_manifest.json", manifest)
    _write_json(plan_dir / "scheduled_command_pack.json", command_pack)
    _write_text(
        plan_dir / "confirmation_cycle_runbook.md",
        render_confirmation_cycle_runbook(manifest, command_pack),
    )
    _write_text(
        plan_dir / "confirmation_cycle_plan_report.md",
        render_confirmation_cycle_plan_report(manifest, command_pack),
    )
    _update_latest_pointer(
        "latest_confirmation_cycle_plan",
        plan_dir.name,
        plan_dir / "confirmation_cycle_plan_manifest.json",
    )
    return {
        "plan_id": plan_dir.name,
        "plan_dir": plan_dir,
        "manifest": manifest,
        "scheduled_command_pack": command_pack,
    }


def confirmation_cycle_plan_report_payload(
    *,
    plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR,
) -> dict[str, Any]:
    plan_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=plan_id if not latest else None,
        pointer_name="latest_confirmation_cycle_plan",
    )
    return {
        **_read_json(plan_dir / "confirmation_cycle_plan_manifest.json"),
        "scheduled_command_pack": _read_json(plan_dir / "scheduled_command_pack.json"),
        "confirmation_cycle_runbook": _read_text(plan_dir / "confirmation_cycle_runbook.md"),
        "confirmation_cycle_plan_report": _read_text(
            plan_dir / "confirmation_cycle_plan_report.md"
        ),
        "plan_dir": str(plan_dir),
    }


def run_confirmation_cycle_weekly(
    *,
    week_ending: date,
    config_path: Path = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    execute_ready_updates: bool = False,
    registry_id: str | None = None,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    outcome_due_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
    outcome_update_review_dir: Path = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    outcome_update_dir: Path = DEFAULT_OUTCOME_UPDATE_DIR,
    rolling_refresh_dir: Path = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    evidence_trend_dir: Path = DEFAULT_EVIDENCE_TREND_DIR,
    forward_decision_dir: Path = DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
    registry_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    evaluation_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    rule_cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    queue_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
    dashboard_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    limited_vs_notrade_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    consensus_risk_dir: Path = DEFAULT_CONSENSUS_RISK_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    paper_portfolio_config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    validation = validate_confirmation_cycle_schedule_config(config_path=config_path)
    if validation["status"] != "PASS":
        raise DynamicV3ConfirmationOperationsError("confirmation cycle config validation failed")
    resolved_registry_id = registry_id or _latest_artifact_id(
        registry_dir,
        "latest_forward_confirmation_registry",
    )
    steps: list[dict[str, Any]] = []
    artifacts: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_cycle_artifacts",
    }
    due = run_outcome_due_scan(
        as_of=week_ending,
        output_dir=outcome_due_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        enforce_data_quality_gate=enforce_data_quality_gate,
        generated_at=generated,
    )
    due_summary = _mapping(due.get("pending_window_summary"))
    artifacts["outcome_due_id"] = due["due_id"]
    steps.append(
        _step(
            "outcome_due_scan",
            "PASS",
            due["due_id"],
            due_windows=_int(due_summary.get("due_windows")),
            update_ready_count=_int(due_summary.get("update_ready_count")),
        )
    )
    review = run_outcome_update_review(
        due_id=due["due_id"],
        output_dir=outcome_update_review_dir,
        outcome_due_dir=outcome_due_dir,
        generated_at=generated,
    )
    review_manifest = _mapping(review.get("manifest"))
    artifacts["outcome_update_review_id"] = review["update_review_id"]
    steps.append(
        _step(
            "outcome_update_review",
            _text(review_manifest.get("status"), "PASS"),
            review["update_review_id"],
            ready_to_update_count=_int(review_manifest.get("ready_to_update_count")),
            blocked_count=_int(review_manifest.get("blocked_count")),
        )
    )
    update: dict[str, Any] | None = None
    if execute_ready_updates and _int(review_manifest.get("ready_to_update_count")) > 0:
        update = run_outcome_update(
            update_review_id=review["update_review_id"],
            output_dir=outcome_update_dir,
            review_dir=outcome_update_review_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            prices_path=prices_path,
            rates_path=rates_path,
            generated_at=generated,
        )
        artifacts["outcome_update_id"] = update["outcome_update_id"]
        steps.append(
            _step(
                "outcome_update",
                _text(_mapping(update.get("manifest")).get("status"), "PASS"),
                update["outcome_update_id"],
                updated_windows=len(update["updated_windows"]),
                skipped_windows=len(update["skipped_windows"]),
            )
        )
    else:
        reason = (
            "execute_ready_updates_false"
            if not execute_ready_updates
            else "no_ready_updates"
        )
        artifacts["outcome_update_id"] = ""
        steps.append(_step("outcome_update", "SKIPPED", "", reason=reason))
    refresh: dict[str, Any] | None = None
    if update is not None:
        refresh = run_rolling_evidence_refresh(
            outcome_update_id=update["outcome_update_id"],
            output_dir=rolling_refresh_dir,
            outcome_update_dir=outcome_update_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            daily_advisory_dir=daily_advisory_dir,
            owner_review_dir=owner_review_dir,
            shadow_shortlist_dir=shadow_shortlist_dir,
            shadow_monitor_run_dir=shadow_monitor_run_dir,
            consensus_drift_dir=consensus_drift_dir,
            config_path=paper_portfolio_config_path,
            outcome_due_dir=outcome_due_dir,
            limited_vs_notrade_dir=limited_vs_notrade_dir,
            consensus_risk_dir=consensus_risk_dir,
            generated_at=generated,
        )
        artifacts["rolling_refresh_id"] = refresh["refresh_id"]
        steps.append(
            _step(
                "rolling_evidence_refresh",
                _text(_mapping(refresh.get("manifest")).get("status"), "PASS"),
                refresh["refresh_id"],
            )
        )
    else:
        artifacts["rolling_refresh_id"] = ""
        steps.append(
            _step(
                "rolling_evidence_refresh",
                "SKIPPED",
                "",
                reason="no_outcome_update_executed",
            )
        )
    trend = run_evidence_trend(
        output_dir=evidence_trend_dir,
        rolling_refresh_dir=rolling_refresh_dir,
        generated_at=generated,
    )
    artifacts["evidence_trend_id"] = trend["trend_id"]
    steps.append(
        _step(
            "evidence_trend",
            _text(_mapping(trend.get("manifest")).get("status"), "PASS"),
            trend["trend_id"],
            trend_status=_text(_mapping(trend.get("confidence_trend_summary")).get("trend_status")),
        )
    )
    progress = update_confirmation_progress(
        registry_id=resolved_registry_id,
        registry_dir=registry_dir,
        output_dir=progress_dir,
        limited_vs_notrade_dir=limited_vs_notrade_dir,
        consensus_risk_dir=consensus_risk_dir,
        generated_at=generated,
    )
    progress_summary = _mapping(progress.get("target_progress_summary"))
    artifacts["confirmation_progress_id"] = progress["progress_id"]
    steps.append(
        _step(
            "confirmation_progress",
            _text(_mapping(progress.get("manifest")).get("status"), "PASS"),
            progress["progress_id"],
            ready_for_evaluation_count=_int(progress_summary.get("ready_for_evaluation_count")),
            insufficient_events_count=_int(progress_summary.get("insufficient_events_count")),
        )
    )
    evaluation = run_confirmation_evaluation(
        progress_id=progress["progress_id"],
        progress_dir=progress_dir,
        output_dir=evaluation_dir,
        generated_at=generated,
    )
    evaluation_summary = _mapping(evaluation.get("confirmation_evaluation_summary"))
    artifacts["confirmation_evaluation_id"] = evaluation["evaluation_id"]
    steps.append(
        _step(
            "confirmation_evaluate",
            _text(_mapping(evaluation.get("manifest")).get("status"), "PASS"),
            evaluation["evaluation_id"],
            success_count=_int(evaluation_summary.get("success_count")),
            failure_count=_int(evaluation_summary.get("failure_count")),
            not_ready_count=_int(evaluation_summary.get("not_ready_count")),
        )
    )
    rule_cycle = run_rule_review_cycle(
        registry_id=resolved_registry_id,
        progress_id=progress["progress_id"],
        evaluation_id=evaluation["evaluation_id"],
        registry_dir=registry_dir,
        progress_dir=progress_dir,
        evaluation_dir=evaluation_dir,
        output_dir=rule_cycle_dir,
        generated_at=generated,
    )
    artifacts["rule_review_cycle_id"] = rule_cycle["cycle_id"]
    cycle_manifest = _mapping(rule_cycle.get("manifest"))
    steps.append(
        _step(
            "rule_review_cycle",
            _text(cycle_manifest.get("status"), "PASS"),
            rule_cycle["cycle_id"],
            cycle_recommendation=_text(cycle_manifest.get("cycle_recommendation")),
            owner_action_count=_int(cycle_manifest.get("targets_requiring_owner_action")),
        )
    )
    queue = build_rule_review_queue(
        cycle_id=rule_cycle["cycle_id"],
        output_dir=queue_dir,
        cycle_dir=rule_cycle_dir,
        generated_at=generated,
    )
    artifacts["rule_review_queue_id"] = queue["queue_id"]
    steps.append(
        _step(
            "owner_decision_queue_update",
            _text(_mapping(queue.get("manifest")).get("status"), "PASS"),
            queue["queue_id"],
            ready_for_owner_review_count=_int(
                _mapping(queue.get("queue_summary")).get("ready_for_owner_review_count")
            ),
        )
    )
    try:
        forward_decision = run_forward_outcome_decision(
            week_ending=week_ending,
            output_dir=forward_decision_dir,
            outcome_update_dir=outcome_update_dir,
            rolling_refresh_dir=rolling_refresh_dir,
            evidence_trend_dir=evidence_trend_dir,
            generated_at=generated,
        )
        artifacts["forward_outcome_decision_id"] = forward_decision["decision_id"]
    except Exception:  # noqa: BLE001
        artifacts["forward_outcome_decision_id"] = ""
        steps.append(
            _step(
                "forward_outcome_decision",
                "SKIPPED",
                "",
                reason="no_outcome_update_or_refresh_artifact",
            )
        )
    dashboard = build_confirmation_dashboard(
        week_ending=week_ending,
        weekly_cycle_id="",
        progress_id=progress["progress_id"],
        evaluation_id=evaluation["evaluation_id"],
        cycle_id=rule_cycle["cycle_id"],
        queue_id=queue["queue_id"],
        output_dir=dashboard_dir,
        weekly_cycle_dir=output_dir,
        progress_dir=progress_dir,
        evaluation_dir=evaluation_dir,
        rule_cycle_dir=rule_cycle_dir,
        queue_dir=queue_dir,
        pressure_tag_dir=pressure_tag_dir,
        generated_at=generated,
    )
    artifacts["confirmation_dashboard_id"] = dashboard["dashboard_id"]
    steps.append(
        _step(
            "weekly_dashboard",
            _text(_mapping(dashboard.get("manifest")).get("status"), "PASS"),
            dashboard["dashboard_id"],
            ready_for_evaluation_count=_int(
                _mapping(dashboard.get("confirmation_dashboard_summary")).get(
                    "ready_for_evaluation"
                )
            ),
        )
    )
    steps.append(_step("reader_brief_update", "PASS", dashboard["dashboard_id"]))
    summary = _weekly_cycle_summary(
        week_ending=week_ending,
        due_summary=due_summary,
        update=update,
        progress_summary=progress_summary,
        evaluation_summary=evaluation_summary,
        cycle_manifest=cycle_manifest,
        queue_summary=_mapping(queue.get("queue_summary")),
    )
    weekly_cycle_id = _stable_id(
        "confirmation-cycle-weekly",
        week_ending.isoformat(),
        generated.isoformat(),
    )
    weekly_dir = _unique_dir(output_dir / weekly_cycle_id)
    weekly_dir.mkdir(parents=True, exist_ok=False)
    artifacts["weekly_cycle_id"] = weekly_dir.name
    summary["weekly_cycle_id"] = weekly_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_cycle_manifest",
        "weekly_cycle_id": weekly_dir.name,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "execute_ready_updates": execute_ready_updates,
        "dry_run": not execute_ready_updates,
        "weekly_cycle_manifest_path": str(weekly_dir / "weekly_cycle_manifest.json"),
        "weekly_cycle_steps_path": str(weekly_dir / "weekly_cycle_steps.json"),
        "weekly_cycle_artifacts_path": str(weekly_dir / "weekly_cycle_artifacts.json"),
        "weekly_cycle_summary_path": str(weekly_dir / "weekly_cycle_summary.json"),
        "weekly_cycle_report_path": str(weekly_dir / "weekly_cycle_report.md"),
        "reader_brief_section_path": str(weekly_dir / "reader_brief_section.md"),
        **_artifact_safety(),
    }
    _write_json(weekly_dir / "weekly_cycle_manifest.json", manifest)
    _write_json(
        weekly_dir / "weekly_cycle_steps.json",
        {"schema_version": SCHEMA_VERSION, "steps": steps},
    )
    _write_json(weekly_dir / "weekly_cycle_artifacts.json", artifacts)
    _write_json(weekly_dir / "weekly_cycle_summary.json", summary)
    _write_text(
        weekly_dir / "weekly_cycle_report.md",
        render_weekly_cycle_report(manifest, steps, summary),
    )
    _write_text(
        weekly_dir / "reader_brief_section.md",
        render_weekly_cycle_reader_brief(summary),
    )
    _update_latest_pointer(
        "latest_confirmation_cycle_weekly",
        weekly_dir.name,
        weekly_dir / "weekly_cycle_manifest.json",
    )
    _patch_dashboard_weekly_cycle_id(
        dashboard_dir=Path(dashboard["dashboard_dir"]),
        weekly_cycle_id=weekly_dir.name,
    )
    return {
        "weekly_cycle_id": weekly_dir.name,
        "weekly_cycle_dir": weekly_dir,
        "manifest": manifest,
        "weekly_cycle_steps": {"steps": steps},
        "weekly_cycle_artifacts": artifacts,
        "weekly_cycle_summary": summary,
    }


def confirmation_cycle_weekly_report_payload(
    *,
    weekly_cycle_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
) -> dict[str, Any]:
    weekly_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=weekly_cycle_id if not latest else None,
        pointer_name="latest_confirmation_cycle_weekly",
    )
    return {
        **_read_json(weekly_dir / "weekly_cycle_manifest.json"),
        "weekly_cycle_steps": _read_json(weekly_dir / "weekly_cycle_steps.json"),
        "weekly_cycle_artifacts": _read_json(weekly_dir / "weekly_cycle_artifacts.json"),
        "weekly_cycle_summary": _read_json(weekly_dir / "weekly_cycle_summary.json"),
        "reader_brief_section": _read_text(weekly_dir / "reader_brief_section.md"),
        "weekly_cycle_dir": str(weekly_dir),
    }


def validate_confirmation_cycle_weekly_artifact(
    *, weekly_cycle_id: str, output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR
) -> dict[str, Any]:
    weekly_dir = output_dir / weekly_cycle_id
    manifest = _read_optional_json(weekly_dir / "weekly_cycle_manifest.json") or {}
    steps_payload = _read_optional_json(weekly_dir / "weekly_cycle_steps.json") or {}
    steps = _records(steps_payload.get("steps"))
    summary = _read_optional_json(weekly_dir / "weekly_cycle_summary.json") or {}
    artifacts = _read_optional_json(weekly_dir / "weekly_cycle_artifacts.json") or {}
    step_names = {_text(row.get("step")) for row in steps}
    checks = [
        _check("manifest_exists", (weekly_dir / "weekly_cycle_manifest.json").exists(), ""),
        _check("steps_exists", (weekly_dir / "weekly_cycle_steps.json").exists(), ""),
        _check("artifacts_exists", (weekly_dir / "weekly_cycle_artifacts.json").exists(), ""),
        _check("summary_exists", (weekly_dir / "weekly_cycle_summary.json").exists(), ""),
        _check("report_exists", (weekly_dir / "weekly_cycle_report.md").exists(), ""),
        _check("reader_brief_exists", (weekly_dir / "reader_brief_section.md").exists(), ""),
        _check("weekly_cycle_id_matches", manifest.get("weekly_cycle_id") == weekly_cycle_id, ""),
        _check(
            "required_steps_present",
            {"outcome_due_scan", "confirmation_progress", "confirmation_evaluate"}
            <= step_names,
            "core weekly steps",
        ),
        _check(
            "dry_run_blocks_default_update",
            manifest.get("execute_ready_updates") is True
            or any(
                row.get("step") == "outcome_update" and row.get("status") == "SKIPPED"
                for row in steps
            ),
            "default update must be skipped",
        ),
        _check("summary_id_matches", summary.get("weekly_cycle_id") == weekly_cycle_id, ""),
        _check("artifact_id_matches", artifacts.get("weekly_cycle_id") == weekly_cycle_id, ""),
        _check(
            "safety_no_broker",
            manifest.get("broker_action_allowed") is False
            and summary.get("broker_action_allowed") is False,
            "broker disabled",
        ),
        _check(
            "production_effect_none",
            manifest.get("production_effect") == "none"
            and summary.get("production_effect") == "none",
            "production none",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_cycle_weekly_validation",
        artifact_id_key="weekly_cycle_id",
        artifact_id=weekly_cycle_id,
        checks=checks,
    )


def run_pressure_regime_tagging(
    *,
    start: date,
    end: date,
    config_path: Path = DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH,
    output_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = _read_yaml_config(config_path)
    _validate_pressure_config_or_raise(config)
    tag_id = _stable_id("pressure-regime-tag", start.isoformat(), end.isoformat(), generated)
    tag_dir = _unique_dir(output_dir / tag_id)
    tag_dir.mkdir(parents=True, exist_ok=False)
    data_quality_status, data_quality_report_path = _pressure_quality_gate(
        as_of=end,
        generated=generated,
        prices_path=prices_path,
        rates_path=rates_path,
        report_path=tag_dir / "validate_data_quality_report.md",
        enforce=enforce_data_quality_gate,
    )
    price_frame = _load_price_frame(prices_path, start=start, end=end)
    window_tags = _build_regime_window_tags(price_frame, config)
    outcome_tags = _build_outcome_regime_tags(
        advisory_outcome_dir=advisory_outcome_dir,
        window_tags=window_tags,
    )
    summary = _pressure_regime_summary(
        window_tags=window_tags,
        outcome_tags=outcome_tags,
        config=config,
        start=start,
        end=end,
    )
    status = "PASS" if window_tags else "INSUFFICIENT_DATA"
    if data_quality_status not in {"PASS", "SKIPPED_EXPLICIT_TEST_FIXTURE"}:
        status = "PASS_WITH_WARNINGS" if window_tags else "INSUFFICIENT_DATA"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_regime_manifest",
        "tag_id": tag_dir.name,
        "generated_at": generated.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "status": status,
        "data_quality_status": data_quality_status,
        "data_quality_report_path": data_quality_report_path,
        "config_path": str(config_path),
        "pressure_regime_manifest_path": str(tag_dir / "pressure_regime_manifest.json"),
        "regime_window_tags_path": str(tag_dir / "regime_window_tags.jsonl"),
        "outcome_regime_tags_path": str(tag_dir / "outcome_regime_tags.jsonl"),
        "pressure_regime_summary_path": str(tag_dir / "pressure_regime_summary.json"),
        "pressure_regime_report_path": str(tag_dir / "pressure_regime_report.md"),
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    _write_json(tag_dir / "pressure_regime_manifest.json", manifest)
    _write_jsonl(tag_dir / "regime_window_tags.jsonl", window_tags)
    _write_jsonl(tag_dir / "outcome_regime_tags.jsonl", outcome_tags)
    _write_json(tag_dir / "pressure_regime_summary.json", summary)
    _write_text(
        tag_dir / "pressure_regime_report.md",
        render_pressure_regime_report(manifest, summary),
    )
    _update_latest_pointer(
        "latest_pressure_regime_tag",
        tag_dir.name,
        tag_dir / "pressure_regime_manifest.json",
    )
    return {
        "tag_id": tag_dir.name,
        "tag_dir": tag_dir,
        "manifest": manifest,
        "regime_window_tags": window_tags,
        "outcome_regime_tags": outcome_tags,
        "pressure_regime_summary": summary,
    }


def pressure_regime_tag_report_payload(
    *,
    tag_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
) -> dict[str, Any]:
    tag_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=tag_id if not latest else None,
        pointer_name="latest_pressure_regime_tag",
    )
    return {
        **_read_json(tag_dir / "pressure_regime_manifest.json"),
        "regime_window_tags": _read_jsonl(tag_dir / "regime_window_tags.jsonl"),
        "outcome_regime_tags": _read_jsonl(tag_dir / "outcome_regime_tags.jsonl"),
        "pressure_regime_summary": _read_json(tag_dir / "pressure_regime_summary.json"),
        "tag_dir": str(tag_dir),
    }


def validate_pressure_regime_tag_artifact(
    *, tag_id: str, output_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR
) -> dict[str, Any]:
    tag_dir = output_dir / tag_id
    manifest = _read_optional_json(tag_dir / "pressure_regime_manifest.json") or {}
    window_tags = _read_jsonl(tag_dir / "regime_window_tags.jsonl")
    outcome_tags = _read_jsonl(tag_dir / "outcome_regime_tags.jsonl")
    summary = _read_optional_json(tag_dir / "pressure_regime_summary.json") or {}
    valid_tags = set(PRESSURE_TAGS)
    checks = [
        _check("manifest_exists", (tag_dir / "pressure_regime_manifest.json").exists(), ""),
        _check("window_tags_exists", (tag_dir / "regime_window_tags.jsonl").exists(), ""),
        _check("outcome_tags_exists", (tag_dir / "outcome_regime_tags.jsonl").exists(), ""),
        _check("summary_exists", (tag_dir / "pressure_regime_summary.json").exists(), ""),
        _check("report_exists", (tag_dir / "pressure_regime_report.md").exists(), ""),
        _check("tag_id_matches", manifest.get("tag_id") == tag_id, ""),
        _check(
            "window_tags_valid",
            all(
                set(_records_to_texts(row.get("regime_tags"))) <= valid_tags
                for row in window_tags
            ),
            "known pressure tags",
        ),
        _check(
            "outcome_tag_status_valid",
            all(
                row.get("tag_status")
                in {"PASS", "PASS_WITH_WARNINGS", "INSUFFICIENT_DATA"}
                for row in outcome_tags
            ),
            "outcome tag status",
        ),
        _check(
            "summary_counts_present",
            all(tag in _mapping(summary.get("pressure_samples")) for tag in PRESSURE_TAGS),
            "pressure sample buckets",
        ),
        _check(
            "safety_no_broker",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker disabled",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_regime_tag_validation",
        artifact_id_key="tag_id",
        artifact_id=tag_id,
        checks=checks,
    )


def build_confirmation_dashboard(
    *,
    week_ending: date,
    weekly_cycle_id: str | None = None,
    progress_id: str | None = None,
    evaluation_id: str | None = None,
    cycle_id: str | None = None,
    queue_id: str | None = None,
    output_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
    weekly_cycle_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    evaluation_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    rule_cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    queue_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    weekly = _optional_weekly_payload(weekly_cycle_id, weekly_cycle_dir)
    progress = _optional_progress_payload(progress_id, progress_dir)
    evaluation = _optional_evaluation_payload(evaluation_id, evaluation_dir)
    rule_cycle = _optional_rule_cycle_payload(cycle_id, rule_cycle_dir)
    queue = _optional_queue_payload(queue_id, queue_dir)
    pressure = _optional_pressure_payload(pressure_tag_dir)
    target_table = _dashboard_target_status_table(progress, evaluation, pressure)
    pressure_dashboard = _pressure_sample_dashboard(pressure)
    summary = _confirmation_dashboard_summary(
        week_ending=week_ending,
        target_table=target_table,
        rule_cycle=rule_cycle,
        queue=queue,
    )
    dashboard_id = _stable_id(
        "confirmation-dashboard",
        week_ending.isoformat(),
        generated.isoformat(),
    )
    dashboard_dir = _unique_dir(output_dir / dashboard_id)
    dashboard_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_dashboard_manifest",
        "dashboard_id": dashboard_dir.name,
        "weekly_cycle_id": _text(weekly_cycle_id or weekly.get("weekly_cycle_id")),
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if target_table["targets"] else "INSUFFICIENT_DATA",
        "confirmation_dashboard_manifest_path": str(
            dashboard_dir / "confirmation_dashboard_manifest.json"
        ),
        "target_status_table_path": str(dashboard_dir / "target_status_table.json"),
        "pressure_sample_dashboard_path": str(
            dashboard_dir / "pressure_sample_dashboard.json"
        ),
        "confirmation_dashboard_summary_path": str(
            dashboard_dir / "confirmation_dashboard_summary.json"
        ),
        "confirmation_dashboard_report_path": str(
            dashboard_dir / "confirmation_dashboard_report.md"
        ),
        "reader_brief_section_path": str(dashboard_dir / "reader_brief_section.md"),
        **_artifact_safety(),
    }
    _write_json(dashboard_dir / "confirmation_dashboard_manifest.json", manifest)
    _write_json(dashboard_dir / "target_status_table.json", target_table)
    _write_json(dashboard_dir / "pressure_sample_dashboard.json", pressure_dashboard)
    _write_json(dashboard_dir / "confirmation_dashboard_summary.json", summary)
    _write_text(
        dashboard_dir / "confirmation_dashboard_report.md",
        render_confirmation_dashboard_report(manifest, target_table, pressure_dashboard, summary),
    )
    _write_text(
        dashboard_dir / "reader_brief_section.md",
        render_confirmation_dashboard_reader_brief(summary, target_table, pressure_dashboard),
    )
    _update_latest_pointer(
        "latest_confirmation_dashboard",
        dashboard_dir.name,
        dashboard_dir / "confirmation_dashboard_manifest.json",
    )
    return {
        "dashboard_id": dashboard_dir.name,
        "dashboard_dir": dashboard_dir,
        "manifest": manifest,
        "target_status_table": target_table,
        "pressure_sample_dashboard": pressure_dashboard,
        "confirmation_dashboard_summary": summary,
    }


def confirmation_dashboard_report_payload(
    *,
    dashboard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
) -> dict[str, Any]:
    dashboard_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=dashboard_id if not latest else None,
        pointer_name="latest_confirmation_dashboard",
    )
    return {
        **_read_json(dashboard_dir / "confirmation_dashboard_manifest.json"),
        "target_status_table": _read_json(dashboard_dir / "target_status_table.json"),
        "pressure_sample_dashboard": _read_json(
            dashboard_dir / "pressure_sample_dashboard.json"
        ),
        "confirmation_dashboard_summary": _read_json(
            dashboard_dir / "confirmation_dashboard_summary.json"
        ),
        "reader_brief_section": _read_text(dashboard_dir / "reader_brief_section.md"),
        "dashboard_dir": str(dashboard_dir),
    }


def validate_confirmation_dashboard_artifact(
    *, dashboard_id: str, output_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR
) -> dict[str, Any]:
    dashboard_dir = output_dir / dashboard_id
    manifest = _read_optional_json(dashboard_dir / "confirmation_dashboard_manifest.json") or {}
    target_table = _read_optional_json(dashboard_dir / "target_status_table.json") or {}
    pressure = _read_optional_json(dashboard_dir / "pressure_sample_dashboard.json") or {}
    summary = _read_optional_json(dashboard_dir / "confirmation_dashboard_summary.json") or {}
    targets = _records(target_table.get("targets"))
    checks = [
        _check(
            "manifest_exists",
            (dashboard_dir / "confirmation_dashboard_manifest.json").exists(),
            "",
        ),
        _check("target_table_exists", (dashboard_dir / "target_status_table.json").exists(), ""),
        _check(
            "pressure_dashboard_exists",
            (dashboard_dir / "pressure_sample_dashboard.json").exists(),
            "",
        ),
        _check(
            "summary_exists",
            (dashboard_dir / "confirmation_dashboard_summary.json").exists(),
            "",
        ),
        _check("report_exists", (dashboard_dir / "confirmation_dashboard_report.md").exists(), ""),
        _check("reader_brief_exists", (dashboard_dir / "reader_brief_section.md").exists(), ""),
        _check("dashboard_id_matches", manifest.get("dashboard_id") == dashboard_id, ""),
        _check("targets_present", bool(targets), "target table"),
        _check(
            "pressure_buckets_present",
            all(tag in _mapping(pressure.get("pressure_samples")) for tag in PRESSURE_TAGS),
            "pressure buckets",
        ),
        _check(
            "policy_change_disallowed",
            summary.get("policy_change_allowed") is False,
            "policy_change_allowed=false",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker disabled",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_dashboard_validation",
        artifact_id_key="dashboard_id",
        artifact_id=dashboard_id,
        checks=checks,
    )


def build_rule_review_queue(
    *,
    cycle_id: str | None = None,
    output_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
    cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    cycle_payload = rule_review_cycle_report_payload(
        cycle_id=cycle_id,
        latest=cycle_id is None,
        output_dir=cycle_dir,
    )
    matrix = _mapping(cycle_payload.get("rule_review_decision_matrix"))
    decisions = _read_jsonl(journal_path)
    items = [
        _queue_item(row, decisions, cycle_payload)
        for row in _records(matrix.get("targets"))
    ]
    summary = _queue_summary(items, generated)
    queue_id = _stable_id(
        "rule-review-queue",
        _text(cycle_payload.get("cycle_id")),
        generated.isoformat(),
    )
    queue_dir = _unique_dir(output_dir / queue_id)
    queue_dir.mkdir(parents=True, exist_ok=False)
    summary["queue_id"] = queue_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_queue_manifest",
        "queue_id": queue_dir.name,
        "source_cycle_id": _text(cycle_payload.get("cycle_id")),
        "generated_at": generated.isoformat(),
        "status": "PASS" if items else "INSUFFICIENT_DATA",
        "rule_review_queue_manifest_path": str(queue_dir / "rule_review_queue_manifest.json"),
        "queue_items_path": str(queue_dir / "queue_items.jsonl"),
        "queue_summary_path": str(queue_dir / "queue_summary.json"),
        "rule_review_queue_report_path": str(queue_dir / "rule_review_queue_report.md"),
        "policy_change_allowed": False,
        **_artifact_safety(),
    }
    _write_json(queue_dir / "rule_review_queue_manifest.json", manifest)
    _write_jsonl(queue_dir / "queue_items.jsonl", items)
    _write_json(queue_dir / "queue_summary.json", summary)
    _write_text(
        queue_dir / "rule_review_queue_report.md",
        render_rule_review_queue_report(manifest, summary, items),
    )
    _update_latest_pointer(
        "latest_rule_review_queue",
        queue_dir.name,
        queue_dir / "rule_review_queue_manifest.json",
    )
    return {
        "queue_id": queue_dir.name,
        "queue_dir": queue_dir,
        "manifest": manifest,
        "queue_items": items,
        "queue_summary": summary,
    }


def rule_review_queue_report_payload(
    *,
    queue_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
) -> dict[str, Any]:
    queue_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=queue_id if not latest else None,
        pointer_name="latest_rule_review_queue",
    )
    return {
        **_read_json(queue_dir / "rule_review_queue_manifest.json"),
        "queue_items": _read_jsonl(queue_dir / "queue_items.jsonl"),
        "queue_summary": _read_json(queue_dir / "queue_summary.json"),
        "rule_review_queue_report": _read_text(queue_dir / "rule_review_queue_report.md"),
        "queue_dir": str(queue_dir),
    }


def validate_rule_review_queue_artifact(
    *, queue_id: str, output_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR
) -> dict[str, Any]:
    queue_dir = output_dir / queue_id
    manifest = _read_optional_json(queue_dir / "rule_review_queue_manifest.json") or {}
    items = _read_jsonl(queue_dir / "queue_items.jsonl")
    summary = _read_optional_json(queue_dir / "queue_summary.json") or {}
    allowed_status = {"pending", "reviewed", "deferred", "not_ready"}
    checks = [
        _check("manifest_exists", (queue_dir / "rule_review_queue_manifest.json").exists(), ""),
        _check("items_exists", (queue_dir / "queue_items.jsonl").exists(), ""),
        _check("summary_exists", (queue_dir / "queue_summary.json").exists(), ""),
        _check("report_exists", (queue_dir / "rule_review_queue_report.md").exists(), ""),
        _check("queue_id_matches", manifest.get("queue_id") == queue_id, ""),
        _check(
            "queue_status_valid",
            all(row.get("queue_status") in allowed_status for row in items),
            "queue status",
        ),
        _check(
            "not_ready_no_owner_action",
            all(
                row.get("recommended_owner_action") != "manual_policy_review"
                for row in items
                if row.get("queue_status") == "not_ready"
            ),
            "not_ready must not require owner action",
        ),
        _check(
            "policy_change_disallowed",
            manifest.get("policy_change_allowed") is False
            and all(row.get("policy_change_allowed") is False for row in items),
            "policy change disabled",
        ),
        _check(
            "summary_counts_match",
            _int(summary.get("pending_count"))
            + _int(summary.get("reviewed_count"))
            + _int(summary.get("deferred_count"))
            + _int(summary.get("not_ready_count"))
            == len(items),
            "queue counts",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker disabled",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rule_review_queue_validation",
        artifact_id_key="queue_id",
        artifact_id=queue_id,
        checks=checks,
    )


def render_confirmation_cycle_runbook(
    manifest: Mapping[str, Any], command_pack: Mapping[str, Any]
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Cycle Weekly Runbook",
        "",
        f"- plan_id: `{manifest.get('plan_id')}`",
        "- cadence: `weekly`",
        "- market_regime: `ai_after_chatgpt`",
        "- default_backtest_start: `2022-12-01`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "- auto_apply_policy: `false`",
        "- owner_approval_required: `true`",
        "",
        "## Command Pack",
    ]
    for row in _records(command_pack.get("commands")):
        lines.extend(
            [
                "",
                f"### {_text(row.get('step'))}",
                f"- command: `{row.get('command')}`",
                f"- required: `{row.get('required')}`",
                f"- execution_mode: `{row.get('execution_mode')}`",
                f"- owner_review_required: `{row.get('owner_review_required')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "Outcome update is skipped unless `--execute-ready-updates` is supplied.",
            "No command in this pack applies policy, mutates production weights, "
            "or triggers broker action.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_confirmation_cycle_plan_report(
    manifest: Mapping[str, Any], command_pack: Mapping[str, Any]
) -> str:
    review_steps = [
        row["step"]
        for row in _records(command_pack.get("commands"))
        if row.get("owner_review_required") is True
    ]
    return (
        "# Dynamic Rescue Confirmation Cycle Plan\n\n"
        f"- plan_id: `{manifest.get('plan_id')}`\n"
        f"- planned_steps: {manifest.get('planned_step_count')}\n"
        "- dry_run_steps: `outcome_update_if_ready` is skipped unless explicitly enabled\n"
        f"- owner_review_steps: `{review_steps}`\n"
        "- policy_auto_apply: `false`\n"
        "- broker_action_allowed: `false`\n"
        "- production_effect: `none`\n"
    )


def render_weekly_cycle_report(
    manifest: Mapping[str, Any],
    steps: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Weekly Cycle",
        "",
        f"- weekly_cycle_id: `{manifest.get('weekly_cycle_id')}`",
        f"- week_ending: `{manifest.get('week_ending')}`",
        f"- dry_run: `{manifest.get('dry_run')}`",
        f"- due_windows: {summary.get('due_windows')}",
        f"- updated_windows: {summary.get('updated_windows')}",
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}",
        f"- failure_count: {summary.get('failure_count')}",
        f"- owner_action_required: `{summary.get('owner_action_required')}`",
        f"- rule_review_recommendation: `{summary.get('rule_review_recommendation')}`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Steps",
    ]
    for row in steps:
        lines.append(
            f"- {_text(row.get('step'))}: `{row.get('status')}` "
            f"artifact=`{row.get('artifact_id')}`"
        )
    return "\n".join(lines) + "\n"


def render_weekly_cycle_reader_brief(summary: Mapping[str, Any]) -> str:
    return (
        "## Dynamic Rescue Confirmation Weekly Cycle\n\n"
        f"- weekly_cycle_id: `{summary.get('weekly_cycle_id')}`\n"
        f"- due_windows: {summary.get('due_windows')}\n"
        f"- updated_windows: {summary.get('updated_windows')}\n"
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}\n"
        f"- rule_review_recommendation: `{summary.get('rule_review_recommendation')}`\n"
        f"- owner_action_required: `{summary.get('owner_action_required')}`\n"
        "- production_effect: `none`\n"
    )


def render_pressure_regime_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any]
) -> str:
    samples = _mapping(summary.get("pressure_samples"))
    return (
        "# Dynamic Rescue Pressure Regime Tagging\n\n"
        f"- tag_id: `{manifest.get('tag_id')}`\n"
        f"- date_range: `{manifest.get('start')}` to `{manifest.get('end')}`\n"
        f"- data_quality_status: `{manifest.get('data_quality_status')}`\n"
        f"- pressure_window_count: {summary.get('pressure_window_count')}\n"
        f"- tech_drawdown_count: {samples.get('tech_drawdown')}\n"
        f"- risk_off_count: {samples.get('risk_off')}\n"
        f"- semiconductor_pullback_count: {samples.get('semiconductor_pullback')}\n"
        f"- pressure_tagged_outcomes: {summary.get('pressure_tagged_outcomes')}\n"
        "- defensive_validation_relevant_outcomes: "
        f"{summary.get('defensive_validation_relevant_outcomes')}\n"
        f"- next_needed_samples: `{summary.get('next_needed_samples')}`\n"
        "- broker_action_allowed: `false`\n"
        "- production_effect: `none`\n"
    )


def render_confirmation_dashboard_report(
    manifest: Mapping[str, Any],
    target_table: Mapping[str, Any],
    pressure_dashboard: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Dashboard",
        "",
        f"- dashboard_id: `{manifest.get('dashboard_id')}`",
        f"- week_ending: `{manifest.get('week_ending')}`",
        f"- targets_total: {summary.get('targets_total')}",
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}",
        f"- dashboard_recommendation: `{summary.get('dashboard_recommendation')}`",
        f"- owner_action_required: `{summary.get('owner_action_required')}`",
        "- policy_change_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Targets",
    ]
    for row in _records(target_table.get("targets")):
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- status: `{row.get('status')}`",
                f"- available_forward_events: {row.get('available_forward_events')}",
                f"- required_forward_events: {row.get('required_forward_events')}",
                f"- available_pressure_events: {row.get('available_pressure_events')}",
                f"- required_pressure_events: {row.get('required_pressure_events')}",
                f"- progress_pct: {row.get('progress_pct')}",
                f"- decision: `{row.get('decision')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Pressure Samples",
            "",
            f"- pressure_samples: `{pressure_dashboard.get('pressure_samples')}`",
            "- defensive_validation_status: "
            f"`{pressure_dashboard.get('defensive_validation_status')}`",
            f"- next_needed_samples: `{pressure_dashboard.get('next_needed_samples')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def render_confirmation_dashboard_reader_brief(
    summary: Mapping[str, Any],
    target_table: Mapping[str, Any],
    pressure_dashboard: Mapping[str, Any],
) -> str:
    targets = {row["target_id"]: row for row in _records(target_table.get("targets"))}
    limited = _mapping(targets.get("limited_adjustment_vs_no_trade"))
    defensive = _mapping(targets.get("defensive_limited_adjustment_drawdown"))
    consensus = _mapping(targets.get("consensus_target_risk"))
    return (
        "## Dynamic Rescue Confirmation Dashboard\n\n"
        f"- targets_total: {summary.get('targets_total')}\n"
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}\n"
        f"- limited_adjustment_progress: {limited.get('progress_pct')}\n"
        f"- defensive_pressure_sample_progress: {defensive.get('progress_pct')}\n"
        f"- consensus_target_status: `{consensus.get('status', 'MISSING')}`\n"
        f"- pressure_samples: `{pressure_dashboard.get('pressure_samples')}`\n"
        f"- dashboard_recommendation: `{summary.get('dashboard_recommendation')}`\n"
        "- production_effect: `none`\n"
    )


def render_rule_review_queue_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Rescue Rule Review Queue",
        "",
        f"- queue_id: `{manifest.get('queue_id')}`",
        f"- source_cycle_id: `{manifest.get('source_cycle_id')}`",
        f"- pending_count: {summary.get('pending_count')}",
        f"- reviewed_count: {summary.get('reviewed_count')}",
        f"- deferred_count: {summary.get('deferred_count')}",
        f"- not_ready_count: {summary.get('not_ready_count')}",
        f"- ready_for_owner_review_count: {summary.get('ready_for_owner_review_count')}",
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`",
        "- policy_change_allowed: `false`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Items",
    ]
    for row in items:
        lines.extend(
            [
                "",
                f"### {_text(row.get('item_id'))}",
                f"- target_id: `{row.get('target_id')}`",
                f"- queue_status: `{row.get('queue_status')}`",
                f"- recommended_owner_action: `{row.get('recommended_owner_action')}`",
                f"- evidence_status: `{row.get('evidence_status')}`",
                f"- summary: {row.get('summary')}",
            ]
        )
    return "\n".join(lines) + "\n"


def _read_yaml_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3ConfirmationOperationsError(f"config not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise DynamicV3ConfirmationOperationsError(f"config root must be mapping: {path}")
    return dict(raw)


def _artifact_safety() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "auto_apply": False,
        "auto_policy_apply": False,
        "policy_change_allowed": False,
        "manual_review_required": True,
        "owner_approval_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _schedule_safety(config: Mapping[str, Any]) -> dict[str, Any]:
    safety = _mapping(config.get("safety"))
    return {
        "broker_action_allowed": safety.get("broker_action_allowed") is True and False,
        "broker_action_taken": False,
        "production_effect": "none",
        "auto_apply_policy": False,
        "owner_approval_required": safety.get("owner_approval_required") is not False,
    }


def _scheduled_commands(config_path: Path) -> list[dict[str, Any]]:
    config_arg = str(config_path).replace("\\", "/")
    rows = [
        (
            "outcome_due_scan",
            "aits etf dynamic-v3-rescue outcome-due scan --as-of <week_ending>",
            "review",
            False,
        ),
        (
            "outcome_update_review",
            "aits etf dynamic-v3-rescue outcome-update-review run --due-id <due_id>",
            "review",
            True,
        ),
        (
            "outcome_update_if_ready",
            "aits etf dynamic-v3-rescue outcome-update run --update-review-id <update_review_id>",
            "explicit_update_only",
            True,
        ),
        (
            "rolling_evidence_refresh",
            "aits etf dynamic-v3-rescue rolling-evidence-refresh run "
            "--outcome-update-id <outcome_update_id>",
            "post_update_review",
            False,
        ),
        (
            "confirmation_progress_update",
            "aits etf dynamic-v3-rescue confirmation-progress update --registry-id <registry_id>",
            "review",
            False,
        ),
        (
            "confirmation_evaluate",
            "aits etf dynamic-v3-rescue confirmation-evaluate run --progress-id <progress_id>",
            "review",
            False,
        ),
        (
            "rule_review_cycle",
            "aits etf dynamic-v3-rescue rule-review-cycle run "
            "--registry-id <registry_id> --progress-id <progress_id> "
            "--evaluation-id <evaluation_id>",
            "review",
            True,
        ),
        (
            "owner_decision_queue_update",
            "aits etf dynamic-v3-rescue rule-review-queue build",
            "review",
            True,
        ),
        (
            "weekly_dashboard",
            "aits etf dynamic-v3-rescue confirmation-dashboard build --week-ending <week_ending>",
            "review",
            False,
        ),
        (
            "reader_brief_update",
            "aits reports reader-brief --latest",
            "read_only",
            False,
        ),
    ]
    return [
        {
            "step": step,
            "command": command.replace(str(config_path), config_arg),
            "required": True,
            "execution_mode": mode,
            "owner_review_required": owner_review,
            "policy_change_allowed": False,
            "broker_action_allowed": False,
            "production_effect": "none",
        }
        for step, command, mode, owner_review in rows
    ]


def _latest_artifact_id(output_dir: Path, pointer_name: str) -> str:
    latest_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None,
        pointer_name=pointer_name,
    )
    return latest_dir.name


def _step(step: str, status: str, artifact_id: str, **summary: Any) -> dict[str, Any]:
    row = {
        "step": step,
        "status": status,
        "artifact_id": artifact_id,
        "summary": summary,
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    if "reason" in summary:
        row["reason"] = summary["reason"]
    return row


def _weekly_cycle_summary(
    *,
    week_ending: date,
    due_summary: Mapping[str, Any],
    update: Mapping[str, Any] | None,
    progress_summary: Mapping[str, Any],
    evaluation_summary: Mapping[str, Any],
    cycle_manifest: Mapping[str, Any],
    queue_summary: Mapping[str, Any],
) -> dict[str, Any]:
    updated = 0
    if update is not None:
        updated = len(_records(update.get("updated_windows")))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_cycle_summary",
        "weekly_cycle_id": "",
        "week_ending": week_ending.isoformat(),
        "due_windows": _int(due_summary.get("due_windows")),
        "updated_windows": updated,
        "forward_available": _int(progress_summary.get("available_forward_events")),
        "forward_pending": _int(due_summary.get("total_pending_windows")),
        "confirmation_targets_total": _int(progress_summary.get("targets_total")),
        "ready_for_evaluation": _int(progress_summary.get("ready_for_evaluation_count")),
        "success_count": _int(evaluation_summary.get("success_count")),
        "failure_count": _int(evaluation_summary.get("failure_count")),
        "not_ready_count": _int(evaluation_summary.get("not_ready_count")),
        "rule_review_recommendation": _text(
            cycle_manifest.get("cycle_recommendation"),
            "continue_tracking",
        ),
        "owner_action_required": _int(queue_summary.get("ready_for_owner_review_count")) > 0,
        "broker_action_allowed": False,
        "production_effect": "none",
        "auto_apply": False,
        "policy_change_allowed": False,
    }


def _pressure_quality_gate(
    *,
    as_of: date,
    generated: datetime,
    prices_path: Path,
    rates_path: Path,
    report_path: Path,
    enforce: bool,
) -> tuple[str, str]:
    if not enforce:
        return "SKIPPED_EXPLICIT_TEST_FIXTURE", ""
    from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
        _quality_gate_for_cached_data,
    )

    return _quality_gate_for_cached_data(
        as_of=as_of,
        generated=generated,
        prices_path=prices_path,
        rates_path=rates_path,
        report_path=report_path,
        enforce=True,
    )


def _validate_pressure_config_or_raise(config: Mapping[str, Any]) -> None:
    validation = _pressure_config_checks(config)
    if not all(row["passed"] for row in validation):
        failed = ", ".join(row["check_id"] for row in validation if not row["passed"])
        raise DynamicV3ConfirmationOperationsError(f"pressure config validation failed: {failed}")


def validate_pressure_regime_tagging_config(
    *, config_path: Path = DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH
) -> dict[str, Any]:
    config = _read_yaml_config(config_path)
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_regime_tagging_config_validation",
        artifact_id_key="config_path",
        artifact_id=str(config_path),
        checks=_pressure_config_checks(config),
    )


def _pressure_config_checks(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    thresholds = _mapping(config.get("thresholds"))
    windows = _mapping(config.get("windows"))
    symbols = _mapping(config.get("symbols"))
    return [
        _check("schema_version_supported", _int(config.get("schema_version")) == 1, "schema=1"),
        _check("tech_proxy_present", bool(_text(symbols.get("tech_proxy"))), "tech proxy"),
        _check(
            "semiconductor_proxy_present",
            bool(_text(symbols.get("semiconductor_proxy"))),
            "semiconductor proxy",
        ),
        _check(
            "thresholds_present",
            all(
                key in thresholds
                for key in (
                    "tech_drawdown_pct",
                    "semiconductor_pullback_pct",
                    "risk_off_volatility_percentile",
                    "strong_recovery_return_pct",
                    "sideways_trend_abs_max",
                )
            ),
            "threshold keys",
        ),
        _check(
            "rolling_windows_present",
            bool([_int(item) for item in windows.get("rolling_days", []) if _int(item) > 0]),
            "rolling days",
        ),
        _check(
            "review_metadata_present",
            bool(_mapping(config.get("policy_metadata")).get("owner"))
            and bool(_mapping(config.get("policy_metadata")).get("version")),
            "policy metadata",
        ),
    ]


def _load_price_frame(prices_path: Path, *, start: date, end: date) -> pd.DataFrame:
    if not prices_path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(prices_path)
    symbol_col = "symbol" if "symbol" in frame.columns else "ticker"
    if "date" not in frame.columns or symbol_col not in frame.columns:
        return pd.DataFrame()
    price_col = "adj_close" if "adj_close" in frame.columns else "close"
    if price_col not in frame.columns:
        return pd.DataFrame()
    frame = frame.rename(columns={symbol_col: "symbol", price_col: "price"}).copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame = frame.dropna(subset=["date", "symbol", "price"])
    return frame[(frame["date"] >= start) & (frame["date"] <= end)].sort_values(
        ["symbol", "date"]
    )


def _build_regime_window_tags(
    price_frame: pd.DataFrame, config: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if price_frame.empty:
        return []
    symbols = _mapping(config.get("symbols"))
    thresholds = _mapping(config.get("thresholds"))
    windows = [_int(item) for item in _mapping(config.get("windows")).get("rolling_days", [])]
    tech_proxy = _text(symbols.get("tech_proxy"), "QQQ")
    semi_proxy = _text(symbols.get("semiconductor_proxy"), "SMH")
    fallback_semi = _text(symbols.get("fallback_semiconductor_proxy"), "SOXX")
    price_map = _symbol_price_map(price_frame)
    semi_symbol = semi_proxy if semi_proxy in price_map else fallback_semi
    base_rows: list[dict[str, Any]] = []
    for window in windows:
        tech_series = price_map.get(tech_proxy, [])
        for index in range(window - 1, len(tech_series)):
            slice_rows = tech_series[index - window + 1 : index + 1]
            start_date = slice_rows[0][0]
            end_date = slice_rows[-1][0]
            qqq_drawdown = _rolling_drawdown(slice_rows)
            qqq_return = _rolling_return(slice_rows)
            vol = _realized_volatility(slice_rows)
            semi_slice = _aligned_slice(price_map.get(semi_symbol, []), start_date, end_date)
            smh_drawdown = _rolling_drawdown(semi_slice)
            smh_return = _rolling_return(semi_slice)
            base_rows.append(
                {
                    "window": window,
                    "start_date": start_date,
                    "end_date": end_date,
                    "qqq_drawdown": qqq_drawdown,
                    "qqq_return": qqq_return,
                    "smh_drawdown": smh_drawdown,
                    "smh_return": smh_return,
                    "realized_volatility": vol,
                    "trend_slope": qqq_return / window if window else 0.0,
                }
            )
    vol_values = sorted(row["realized_volatility"] for row in base_rows)
    vol_threshold = _percentile(
        vol_values,
        _float(thresholds.get("risk_off_volatility_percentile")),
    )
    tagged = []
    for row in base_rows:
        tags = _regime_tags_for_metrics(row, thresholds, vol_threshold)
        tagged.append(
            {
                "window_id": _stable_id(
                    "pressure-window",
                    row["start_date"].isoformat(),
                    row["end_date"].isoformat(),
                    row["window"],
                ),
                "start_date": row["start_date"].isoformat(),
                "end_date": row["end_date"].isoformat(),
                "window_days": row["window"],
                "regime_tags": tags,
                "metrics": {
                    "qqq_drawdown": round(row["qqq_drawdown"], 6),
                    "smh_drawdown": round(row["smh_drawdown"], 6),
                    "realized_volatility": round(row["realized_volatility"], 6),
                    "trend_slope": round(row["trend_slope"], 6),
                    "qqq_return": round(row["qqq_return"], 6),
                    "smh_return": round(row["smh_return"], 6),
                },
                "tag_confidence": "HIGH" if tags else "LOW",
                "production_effect": "none",
                "broker_action_allowed": False,
            }
        )
    return tagged


def _build_outcome_regime_tags(
    *, advisory_outcome_dir: Path, window_tags: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    tags_by_end: dict[tuple[str, int], list[str]] = {}
    for row in window_tags:
        tags_by_end[(_text(row.get("end_date")), _int(row.get("window_days")))] = (
            _records_to_texts(row.get("regime_tags"))
        )
    rows = []
    for manifest_path in sorted(advisory_outcome_dir.glob("*/advisory_outcome_manifest.json")):
        manifest = _read_optional_json(manifest_path) or {}
        outcome_id = _text(manifest.get("outcome_id"), manifest_path.parent.name)
        windows = _read_jsonl(manifest_path.parent / "outcome_windows.jsonl")
        for window in windows:
            end_date = _text(window.get("end_date"))
            window_days = _int(window.get("window_days"))
            regime_tags = tags_by_end.get((end_date, window_days), [])
            pressure = bool(set(regime_tags) & PRESSURE_VALIDATION_TAGS)
            rows.append(
                {
                    "outcome_id": outcome_id,
                    "daily_advisory_id": _text(
                        window.get("daily_advisory_id") or manifest.get("daily_advisory_id")
                    ),
                    "as_of": _text(window.get("start_date") or manifest.get("as_of")),
                    "window_days": window_days,
                    "regime_tags": regime_tags,
                    "pressure_regime": pressure,
                    "defensive_validation_relevant": pressure and window_days in {5, 10, 20},
                    "tag_status": "PASS" if regime_tags else "INSUFFICIENT_DATA",
                    "production_effect": "none",
                    "broker_action_allowed": False,
                }
            )
    return rows


def _pressure_regime_summary(
    *,
    window_tags: Sequence[Mapping[str, Any]],
    outcome_tags: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    start: date,
    end: date,
) -> dict[str, Any]:
    sample_counts = Counter(
        tag for row in window_tags for tag in _records_to_texts(row.get("regime_tags"))
    )
    pressure_window_count = sum(
        1
        for row in window_tags
        if set(_records_to_texts(row.get("regime_tags"))) & PRESSURE_VALIDATION_TAGS
    )
    defensive_count = sum(
        1 for row in outcome_tags if row.get("defensive_validation_relevant") is True
    )
    required = _int(_mapping(config.get("validation")).get("required_pressure_events"), 5)
    next_needed = [
        tag
        for tag in ("tech_drawdown", "semiconductor_pullback", "risk_off")
        if sample_counts.get(tag, 0) <= 0
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_regime_summary",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "pressure_window_count": pressure_window_count,
        "pressure_samples": {tag: sample_counts.get(tag, 0) for tag in PRESSURE_TAGS},
        "pressure_tagged_outcomes": sum(
            1 for row in outcome_tags if row.get("pressure_regime") is True
        ),
        "defensive_validation_relevant_outcomes": defensive_count,
        "required_pressure_events": required,
        "defensive_validation_status": (
            "READY_FOR_REVIEW" if defensive_count >= required else "INSUFFICIENT_PRESSURE_EVENTS"
        ),
        "next_needed_samples": next_needed,
        "future_outcomes_to_watch": [
            row.get("outcome_id")
            for row in outcome_tags
            if row.get("tag_status") == "INSUFFICIENT_DATA"
        ][:10],
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _symbol_price_map(frame: pd.DataFrame) -> dict[str, list[tuple[date, float]]]:
    result: dict[str, list[tuple[date, float]]] = {}
    for symbol, group in frame.groupby("symbol"):
        result[str(symbol)] = [
            (row["date"], float(row["price"]))
            for row in group.sort_values("date").to_dict("records")
            if float(row["price"]) > 0
        ]
    return result


def _aligned_slice(
    series: Sequence[tuple[date, float]], start_date: date, end_date: date
) -> list[tuple[date, float]]:
    return [(day, price) for day, price in series if start_date <= day <= end_date]


def _rolling_return(rows: Sequence[tuple[date, float]]) -> float:
    if len(rows) < 2 or rows[0][1] == 0:
        return 0.0
    return rows[-1][1] / rows[0][1] - 1.0


def _rolling_drawdown(rows: Sequence[tuple[date, float]]) -> float:
    if not rows:
        return 0.0
    peak = max(price for _, price in rows)
    if peak <= 0:
        return 0.0
    return rows[-1][1] / peak - 1.0


def _realized_volatility(rows: Sequence[tuple[date, float]]) -> float:
    returns = []
    for (_, prior), (_, current) in zip(rows, rows[1:], strict=False):
        if prior > 0:
            returns.append(current / prior - 1.0)
    if len(returns) < 2:
        return 0.0
    series = pd.Series(returns)
    return float(series.std(ddof=0) * math.sqrt(252))


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    clipped = min(max(percentile, 0.0), 1.0)
    index = min(len(values) - 1, int(round((len(values) - 1) * clipped)))
    return values[index]


def _regime_tags_for_metrics(
    row: Mapping[str, Any], thresholds: Mapping[str, Any], vol_threshold: float
) -> list[str]:
    tags = []
    qqq_drawdown = _float(row.get("qqq_drawdown"))
    smh_drawdown = _float(row.get("smh_drawdown"))
    qqq_return = _float(row.get("qqq_return"))
    smh_return = _float(row.get("smh_return"))
    vol = _float(row.get("realized_volatility"))
    slope = _float(row.get("trend_slope"))
    tech_threshold = _float(thresholds.get("tech_drawdown_pct"))
    semi_threshold = _float(thresholds.get("semiconductor_pullback_pct"))
    if qqq_drawdown <= tech_threshold:
        tags.append("tech_drawdown")
    if smh_drawdown <= semi_threshold:
        tags.append("semiconductor_pullback")
    if qqq_drawdown <= tech_threshold and vol >= vol_threshold:
        tags.append("risk_off")
    if abs(slope) <= _float(thresholds.get("sideways_trend_abs_max")) and vol >= vol_threshold:
        tags.append("sideways_choppy")
    if qqq_return >= _float(thresholds.get("strong_recovery_return_pct")):
        tags.append("strong_recovery")
    if qqq_return > 0 and smh_return >= 0 and qqq_drawdown > tech_threshold:
        tags.append("ai_trend")
    return tags


def _optional_weekly_payload(weekly_cycle_id: str | None, output_dir: Path) -> dict[str, Any]:
    if weekly_cycle_id == "":
        return {}
    try:
        return confirmation_cycle_weekly_report_payload(
            weekly_cycle_id=weekly_cycle_id,
            latest=weekly_cycle_id is None,
            output_dir=output_dir,
        )
    except Exception:  # noqa: BLE001
        return {}


def _optional_progress_payload(progress_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        return confirmation_progress_report_payload(
            progress_id=progress_id,
            latest=progress_id is None,
            output_dir=output_dir,
        )
    except Exception:  # noqa: BLE001
        return {}


def _optional_evaluation_payload(evaluation_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        return confirmation_evaluation_report_payload(
            evaluation_id=evaluation_id,
            latest=evaluation_id is None,
            output_dir=output_dir,
        )
    except Exception:  # noqa: BLE001
        return {}


def _optional_rule_cycle_payload(cycle_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        return rule_review_cycle_report_payload(
            cycle_id=cycle_id,
            latest=cycle_id is None,
            output_dir=output_dir,
        )
    except Exception:  # noqa: BLE001
        return {}


def _optional_queue_payload(queue_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        return rule_review_queue_report_payload(
            queue_id=queue_id,
            latest=queue_id is None,
            output_dir=output_dir,
        )
    except Exception:  # noqa: BLE001
        return {}


def _optional_pressure_payload(output_dir: Path) -> dict[str, Any]:
    try:
        return pressure_regime_tag_report_payload(latest=True, output_dir=output_dir)
    except Exception:  # noqa: BLE001
        return {}


def _dashboard_target_status_table(
    progress: Mapping[str, Any],
    evaluation: Mapping[str, Any],
    pressure: Mapping[str, Any],
) -> dict[str, Any]:
    eval_by_target = {
        _text(row.get("target_id")): row for row in _records(evaluation.get("target_evaluations"))
    }
    pressure_summary = _mapping(pressure.get("pressure_regime_summary"))
    defensive_count = _int(pressure_summary.get("defensive_validation_relevant_outcomes"))
    targets = []
    for row in _records(progress.get("target_progress")):
        target_id = _text(row.get("target_id"))
        evaluation_row = _mapping(eval_by_target.get(target_id))
        required_forward = _int(row.get("required_forward_events"))
        required_pressure = _int(row.get("required_pressure_regime_events"))
        available_forward = _int(row.get("available_forward_events"))
        available_pressure = (
            defensive_count
            if target_id == "defensive_limited_adjustment_drawdown"
            else _int(row.get("available_pressure_regime_events"))
        )
        required = required_pressure or required_forward
        available = available_pressure if required_pressure else available_forward
        progress_pct = round(available / required, 4) if required else 0.0
        targets.append(
            {
                "target_id": target_id,
                "status": _text(row.get("progress_status")),
                "target_status": _text(row.get("target_status")),
                "available_forward_events": available_forward,
                "required_forward_events": required_forward,
                "available_pressure_events": available_pressure,
                "required_pressure_events": required_pressure,
                "progress_pct": min(progress_pct, 1.0),
                "current_win_rate": _mapping(row.get("current_metrics")).get(
                    "win_rate_vs_no_trade"
                ),
                "current_avg_relative_return": _mapping(row.get("current_metrics")).get(
                    "avg_relative_return"
                ),
                "current_drawdown_delta": _mapping(row.get("current_metrics")).get(
                    "drawdown_delta"
                ),
                "decision": _text(evaluation_row.get("recommendation"), "continue_tracking"),
                "policy_change_allowed": False,
                "broker_action_allowed": False,
                "production_effect": "none",
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_target_status_table",
        "targets": targets,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_sample_dashboard(pressure: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(pressure.get("pressure_regime_summary"))
    samples = _mapping(summary.get("pressure_samples"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_sample_dashboard",
        "pressure_samples": {tag: _int(samples.get(tag)) for tag in PRESSURE_TAGS},
        "defensive_validation_status": _text(
            summary.get("defensive_validation_status"),
            "INSUFFICIENT_PRESSURE_EVENTS",
        ),
        "next_needed_samples": summary.get(
            "next_needed_samples",
            ["tech_drawdown", "semiconductor_pullback", "risk_off"],
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _confirmation_dashboard_summary(
    *,
    week_ending: date,
    target_table: Mapping[str, Any],
    rule_cycle: Mapping[str, Any],
    queue: Mapping[str, Any],
) -> dict[str, Any]:
    targets = _records(target_table.get("targets"))
    ready = sum(1 for row in targets if row.get("status") == "READY_FOR_EVALUATION")
    queue_summary = _mapping(queue.get("queue_summary"))
    recommendation = _text(rule_cycle.get("cycle_recommendation"), "continue_tracking")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_dashboard_summary",
        "week_ending": week_ending.isoformat(),
        "targets_total": len(targets),
        "ready_for_evaluation": ready,
        "continue_tracking": sum(
            1 for row in targets if row.get("decision") == "continue_tracking"
        ),
        "owner_action_required": _int(queue_summary.get("ready_for_owner_review_count")) > 0,
        "policy_change_allowed": False,
        "dashboard_recommendation": recommendation,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _queue_item(
    row: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    cycle_payload: Mapping[str, Any],
) -> dict[str, Any]:
    target_id = _text(row.get("target_id"))
    reviewed = [
        item
        for item in decisions
        if target_id in _records_to_texts(item.get("target_ids"))
        and _text(item.get("owner_decision")) not in {"", "pending"}
    ]
    decision = _text(row.get("rule_review_decision"))
    owner_action = row.get("owner_action_required") is True
    if reviewed:
        status = "reviewed"
    elif owner_action:
        status = "pending"
    elif decision == "DEFER":
        status = "deferred"
    else:
        status = "not_ready"
    evidence = "READY_FOR_REVIEW" if owner_action else "NOT_READY"
    recommended = _recommended_owner_action(decision, status)
    return {
        "item_id": _stable_id(
            "rule-review-queue-item",
            _text(cycle_payload.get("cycle_id")),
            target_id,
        ),
        "target_id": target_id,
        "source_cycle_id": _text(cycle_payload.get("cycle_id")),
        "source_evaluation_id": _text(cycle_payload.get("evaluation_id")),
        "queue_status": status,
        "recommended_owner_action": recommended,
        "evidence_status": evidence,
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "summary": _text(row.get("reason")),
        "production_effect": "none",
    }


def _recommended_owner_action(decision: str, status: str) -> str:
    if status == "reviewed":
        return "continue_tracking"
    if decision in {
        "READY_FOR_OWNER_REVIEW",
        "TIGHTEN_RULES_RECOMMENDED",
        "LOOSEN_RULES_RECOMMENDED",
        "RENAME_OR_RECLASSIFY",
    }:
        return "manual_policy_review"
    if decision == "KEEP_REFERENCE_ONLY":
        return "keep_reference_only"
    if decision == "DEFER":
        return "request_more_data"
    return "continue_tracking"


def _queue_summary(items: Sequence[Mapping[str, Any]], generated: datetime) -> dict[str, Any]:
    counts = Counter(_text(row.get("queue_status")) for row in items)
    ready = sum(1 for row in items if row.get("evidence_status") == "READY_FOR_REVIEW")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_queue_summary",
        "queue_id": "",
        "generated_at": generated.isoformat(),
        "pending_count": counts.get("pending", 0),
        "reviewed_count": counts.get("reviewed", 0),
        "deferred_count": counts.get("deferred", 0),
        "not_ready_count": counts.get("not_ready", 0),
        "ready_for_owner_review_count": ready,
        "summary_recommendation": "owner_review_required" if ready else "continue_tracking",
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _records_to_texts(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return []
    return [_text(item) for item in value if _text(item)]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _patch_dashboard_weekly_cycle_id(*, dashboard_dir: Path, weekly_cycle_id: str) -> None:
    manifest_path = dashboard_dir / "confirmation_dashboard_manifest.json"
    if not manifest_path.exists():
        return
    manifest = _read_json(manifest_path)
    manifest["weekly_cycle_id"] = weekly_cycle_id
    _write_json(manifest_path, manifest)
