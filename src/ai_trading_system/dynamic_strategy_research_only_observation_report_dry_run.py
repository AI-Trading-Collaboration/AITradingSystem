from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_research_only_observation_log_schema_plan import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_observation_log_schema_plan import (
    NEXT_ROUTE as SOURCE_2372_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_only_observation_log_schema_plan import (
    READY_STATUS as SOURCE_2372_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    NEXT_ROUTE as SOURCE_2371_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    OWNER_DECISION as SOURCE_2371_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    READY_STATUS as SOURCE_2371_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    NEXT_ROUTE as SOURCE_2369_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    OBSERVATION_MODE as SOURCE_2369_OBSERVATION_MODE,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    READY_STATUS as SOURCE_2369_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    NEXT_ROUTE as SOURCE_2370_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    READY_STATUS as SOURCE_2370_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2373"
TASK_REGISTER_ID = (
    "TRADING-2373_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN"
)
REPORT_TYPE = "dynamic_strategy_research_only_observation_report_dry_run"
SCHEMA_VERSION = "dynamic_strategy_research_only_observation_report_dry_run.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_"
    "BLOCKED_SOURCE_ARTIFACT"
)
REPORT_MODE = "RESEARCH_ONLY_MANUAL_DRY_RUN"
NEXT_ROUTE = (
    "TRADING-2374_Dynamic_Strategy_Research_Only_Observation_"
    "Owner_Reassessment_Checkpoint"
)
PRIMARY_CANDIDATE_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
OBSERVATION_DECISION_REQUIRED = "OWNER_REVIEW_REQUIRED"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2369",
    "TRADING-2370",
    "TRADING-2371",
    "TRADING-2372",
)

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2371_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision_result.json"
)
DEFAULT_SOURCE_2372_LOG_SCHEMA_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_OUTPUT_ROOT
    / "log_schema_plan_result.json"
)
DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT
    / "observation_dry_run_result.json"
)
DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RECORD_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT
    / "observation_dry_run_record.json"
)
DEFAULT_SOURCE_2370_REPLAY_VALIDATION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT
    / "replay_validation_result.json"
)


def run_dynamic_strategy_research_only_observation_report_dry_run(
    *,
    source_owner_review_decision_path: Path = (
        DEFAULT_SOURCE_2371_OWNER_REVIEW_DECISION_PATH
    ),
    source_log_schema_plan_path: Path = DEFAULT_SOURCE_2372_LOG_SCHEMA_PLAN_PATH,
    source_observation_dry_run_result_path: Path = (
        DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RESULT_PATH
    ),
    source_observation_dry_run_record_path: Path = (
        DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RECORD_PATH
    ),
    source_replay_validation_path: Path = DEFAULT_SOURCE_2370_REPLAY_VALIDATION_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_owner_review_decision_path=source_owner_review_decision_path,
        source_log_schema_plan_path=source_log_schema_plan_path,
        source_observation_dry_run_result_path=source_observation_dry_run_result_path,
        source_observation_dry_run_record_path=source_observation_dry_run_record_path,
        source_replay_validation_path=source_replay_validation_path,
    )
    validation_errors = _source_validation_errors(sources)
    ready = not validation_errors
    resolved_as_of = _resolve_as_of(as_of_date, sources)
    record_example = _observation_record_example(
        sources=sources,
        as_of_date=resolved_as_of,
    )
    report_dry_run = _observation_report_dry_run(
        sources=sources,
        record_example=record_example,
        ready=ready,
    )
    no_side_effect_evidence = _no_side_effect_evidence()
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        validation_errors=validation_errors,
        record_example=record_example,
        report_dry_run=report_dry_run,
        evidence=no_side_effect_evidence,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_owner_review_decision_path: Path,
    source_log_schema_plan_path: Path,
    source_observation_dry_run_result_path: Path,
    source_observation_dry_run_record_path: Path,
    source_replay_validation_path: Path,
) -> dict[str, Any]:
    owner_decision = _load_json_document(source_owner_review_decision_path)
    log_schema_plan = _load_json_document(source_log_schema_plan_path)
    dry_run_result = _load_json_document(source_observation_dry_run_result_path)
    dry_run_record_doc = _load_json_document(source_observation_dry_run_record_path)
    replay_validation = _load_json_document(source_replay_validation_path)
    return {
        "owner_decision": _as_mapping(owner_decision),
        "log_schema_plan": _as_mapping(log_schema_plan),
        "dry_run_result": _as_mapping(dry_run_result),
        "dry_run_record_doc": _as_mapping(dry_run_record_doc),
        "replay_validation": _as_mapping(replay_validation),
        "source_artifacts": {
            "owner_decision": _source_artifact(
                source_owner_review_decision_path,
                owner_decision,
            ),
            "log_schema_plan": _source_artifact(
                source_log_schema_plan_path,
                log_schema_plan,
            ),
            "observation_dry_run_result": _source_artifact(
                source_observation_dry_run_result_path,
                dry_run_result,
            ),
            "observation_dry_run_record": _source_artifact(
                source_observation_dry_run_record_path,
                dry_run_record_doc,
            ),
            "replay_validation": _source_artifact(
                source_replay_validation_path,
                replay_validation,
            ),
        },
    }


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    owner_decision = _as_mapping(sources.get("owner_decision"))
    log_schema_plan = _as_mapping(sources.get("log_schema_plan"))
    dry_run = _as_mapping(sources.get("dry_run_result"))
    dry_run_record_doc = _as_mapping(sources.get("dry_run_record_doc"))
    replay = _as_mapping(sources.get("replay_validation"))
    source_status = _source_status(sources)

    if source_status.get("owner_decision") != SOURCE_2371_READY_STATUS:
        errors.append("owner_decision_status_not_ready")
    if source_status.get("log_schema_plan") != SOURCE_2372_READY_STATUS:
        errors.append("log_schema_plan_status_not_ready")
    if source_status.get("observation_dry_run_result") != SOURCE_2369_READY_STATUS:
        errors.append("observation_dry_run_result_status_not_ready")
    if source_status.get("observation_dry_run_record") != SOURCE_2369_READY_STATUS:
        errors.append("observation_dry_run_record_status_not_ready")
    if source_status.get("replay_validation") != SOURCE_2370_READY_STATUS:
        errors.append("replay_validation_status_not_ready")

    if owner_decision.get("next_route") != SOURCE_2371_EXPECTED_NEXT_ROUTE:
        errors.append("owner_decision_next_route_not_trading_2372")
    if log_schema_plan.get("next_route") != SOURCE_2372_EXPECTED_NEXT_ROUTE:
        errors.append("log_schema_plan_next_route_not_trading_2373")
    if dry_run.get("next_route") != SOURCE_2369_EXPECTED_NEXT_ROUTE:
        errors.append("dry_run_next_route_not_trading_2370")
    if replay.get("next_route") != SOURCE_2370_EXPECTED_NEXT_ROUTE:
        errors.append("replay_validation_next_route_not_trading_2371")

    if owner_decision.get("owner_decision") != SOURCE_2371_OWNER_DECISION:
        errors.append("owner_decision_not_research_only_continue")
    if owner_decision.get("owner_review_decision_recorded") is not True:
        errors.append("owner_review_decision_not_recorded")
    if owner_decision.get("research_only_observation_continue_allowed") is not True:
        errors.append("research_only_observation_continue_not_allowed")

    if log_schema_plan.get("observation_log_schema_ready") is not True:
        errors.append("observation_log_schema_not_ready")
    if log_schema_plan.get("observation_report_plan_ready") is not True:
        errors.append("observation_report_plan_not_ready")
    if log_schema_plan.get("schema_only") is not True:
        errors.append("log_schema_plan_not_schema_only")
    if log_schema_plan.get("report_plan_only") is not True:
        errors.append("log_schema_plan_not_report_plan_only")
    if bool(log_schema_plan.get("periodic_daily_report_generated")):
        errors.append("periodic_daily_report_generated_true")
    if bool(log_schema_plan.get("event_log_written")):
        errors.append("event_log_written_true")

    if dry_run.get("observation_mode") != SOURCE_2369_OBSERVATION_MODE:
        errors.append("dry_run_mode_not_research_only")
    if dry_run.get("observation_decision") != OBSERVATION_DECISION_REQUIRED:
        errors.append("dry_run_observation_decision_not_owner_review_required")
    if dry_run.get("owner_review_required") is not True:
        errors.append("dry_run_owner_review_required_not_true")
    if dry_run.get("observation_dry_run_record_ready") is not True:
        errors.append("dry_run_record_not_ready")
    if dry_run.get("no_side_effect_evidence_ready") is not True:
        errors.append("dry_run_no_side_effect_evidence_not_ready")
    if not _as_mapping(dry_run_record_doc.get("observation_dry_run_record")):
        errors.append("observation_dry_run_record_missing")

    if replay.get("stable_semantic_replay_passed") is not True:
        errors.append("replay_stable_semantic_not_passed")
    if replay.get("no_side_effect_evidence_ready") is not True:
        errors.append("replay_no_side_effect_evidence_not_ready")
    if replay.get("observation_decision") != OBSERVATION_DECISION_REQUIRED:
        errors.append("replay_observation_decision_not_owner_review_required")
    if replay.get("owner_review_required") is not True:
        errors.append("replay_owner_review_required_not_true")

    for source_name, source in (
        ("owner_decision", owner_decision),
        ("log_schema_plan", log_schema_plan),
        ("dry_run", dry_run),
        ("replay_validation", replay),
    ):
        for field in _side_effect_false_fields():
            if bool(source.get(field)):
                errors.append(f"{source_name}_{field}_true")
        if bool(source.get("order_generated")):
            errors.append(f"{source_name}_order_generated_true")
        if source.get("broker_action") not in (None, "none"):
            errors.append(f"{source_name}_broker_action_not_none")
        if source.get("production_effect") not in (None, "none"):
            errors.append(f"{source_name}_production_effect_not_none")
    return errors


def _observation_record_example(
    *,
    sources: Mapping[str, Any],
    as_of_date: date,
) -> dict[str, Any]:
    owner_decision = _as_mapping(sources.get("owner_decision"))
    dry_run = _as_mapping(sources.get("dry_run_result"))
    dry_run_record = _as_mapping(
        _as_mapping(sources.get("dry_run_record_doc")).get("observation_dry_run_record")
    )
    identity = _as_mapping(dry_run_record.get("identity"))
    signal_state = _as_mapping(dry_run_record.get("signal_state"))
    portfolio_preview = _as_mapping(dry_run_record.get("portfolio_preview"))
    cost_and_turnover = _as_mapping(dry_run_record.get("cost_and_turnover"))
    comparison = _as_mapping(dry_run_record.get("comparison"))
    review = _as_mapping(dry_run_record.get("review"))
    candidate = _primary_candidate(sources)
    observation_id = f"{TASK_ID}_{as_of_date.isoformat()}_{candidate}"
    return {
        "identity": {
            "observation_id": observation_id,
            "as_of": as_of_date.isoformat(),
            "generated_by_task": TASK_ID,
            "source_artifact": str(
                _as_mapping(sources.get("source_artifacts"))
                .get("observation_dry_run_record", {})
                .get("path")
            ),
            "candidate_id": candidate,
            "candidate_version": identity.get(
                "candidate_version",
                "source_artifact_candidate_from_trading_2369",
            ),
            "execution_cadence": dry_run.get("execution_cadence")
            or identity.get("execution_cadence"),
        },
        "candidate_context": {
            "primary_observation_candidate": candidate,
            "ranking_top_from_2365": dry_run.get("ranking_top_from_2365")
            or RANKING_TOP_FALLBACK,
            "robustness_top_from_2366": dry_run.get("robustness_top_from_2366")
            or PRIMARY_CANDIDATE_FALLBACK,
            "gate_decision_from_2367": dry_run.get("gate_decision_from_2367")
            or OBSERVATION_DECISION_REQUIRED,
            "owner_decision_from_2371": owner_decision.get("owner_decision"),
        },
        "signal_context": {
            "signal_state": signal_state.get("signal_state"),
            "advisory_valid_from": signal_state.get("advisory_valid_from"),
            "advisory_valid_until": signal_state.get("advisory_valid_until"),
            "signal_horizon": signal_state.get("signal_horizon"),
            "valid_until_window_state": _valid_until_window_state(signal_state),
        },
        "portfolio_preview": {
            "reference_weight": portfolio_preview.get("reference_weight"),
            "proposed_research_weight": portfolio_preview.get(
                "proposed_research_weight"
            ),
            "proposed_weight_delta": portfolio_preview.get("proposed_weight_delta"),
            "risk_cap_state": portfolio_preview.get("risk_cap_state"),
            "constraint_state": portfolio_preview.get("constraint_state"),
            "cooldown_state": portfolio_preview.get("cooldown_state"),
            "no_trade_reason": portfolio_preview.get("no_trade_reason"),
        },
        "cost_turnover": {
            "expected_turnover": cost_and_turnover.get("expected_turnover"),
            "transaction_cost_bps": cost_and_turnover.get("transaction_cost_bps"),
            "slippage_bps": cost_and_turnover.get("slippage_bps"),
            "estimated_cost_drag": cost_and_turnover.get("estimated_cost_drag"),
            "turnover_cap_state": cost_and_turnover.get("turnover_cap_state"),
        },
        "comparison": {
            "static_baseline_comparison": comparison.get("static_baseline_comparison"),
            "ranking_top_candidate_comparison": comparison.get(
                "ranking_top_candidate_comparison"
            ),
            "robustness_top_candidate_comparison": comparison.get(
                "robustness_top_candidate_comparison"
            ),
            "dynamic_vs_static_preview_gap": comparison.get(
                "dynamic_vs_static_preview_gap"
            ),
        },
        "review": {
            "observation_decision": review.get("observation_decision")
            or OBSERVATION_DECISION_REQUIRED,
            "owner_review_required": review.get("owner_review_required", True),
            "review_reason": review.get("review_reason"),
            "escalation_flag": review.get("escalation_flag"),
        },
        "guardrails": _guardrails(),
    }


def _observation_report_dry_run(
    *,
    sources: Mapping[str, Any],
    record_example: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    log_schema_plan = _as_mapping(sources.get("log_schema_plan"))
    dry_run = _as_mapping(sources.get("dry_run_result"))
    plan = _as_mapping(log_schema_plan.get("observation_report_plan"))
    sections = _as_list(plan.get("sections"))
    review = _as_mapping(record_example.get("review"))
    signal = _as_mapping(record_example.get("signal_context"))
    portfolio = _as_mapping(record_example.get("portfolio_preview"))
    cost_turnover = _as_mapping(record_example.get("cost_turnover"))
    comparison = _as_mapping(record_example.get("comparison"))
    return {
        "schema_version": "dynamic_strategy_research_only_observation_report_dry_run.v1",
        "report_mode": REPORT_MODE,
        "dry_run_ready": ready,
        "sections": sections,
        "section_payloads": {
            "Executive summary": {
                "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
                "primary_observation_candidate": _primary_candidate(sources),
                "observation_decision": review.get("observation_decision"),
                "owner_review_required": review.get("owner_review_required"),
                "daily_report_generated": False,
            },
            "Candidate under observation": {
                "candidate_context": record_example.get("candidate_context"),
            },
            "Signal / valid-until status": signal,
            "Portfolio preview": portfolio,
            "Static baseline comparison": comparison.get(
                "static_baseline_comparison"
            ),
            "Ranking top vs robustness top comparison": {
                "ranking_top_candidate_comparison": comparison.get(
                    "ranking_top_candidate_comparison"
                ),
                "robustness_top_candidate_comparison": comparison.get(
                    "robustness_top_candidate_comparison"
                ),
            },
            "Cost / turnover / cooldown status": cost_turnover,
            "Review flags": review,
            "Guardrail summary": _guardrails(),
            "Explicit non-goals": {
                "not_daily_report": True,
                "not_scheduler": True,
                "not_event_append": True,
                "not_outcome_binding": True,
                "not_paper_shadow": True,
                "not_production": True,
                "not_broker": True,
            },
        },
        "source_observation_mode": dry_run.get("observation_mode"),
        "source_schema_version": log_schema_plan.get("schema_version"),
    }


def _no_side_effect_evidence() -> dict[str, Any]:
    return {
        "schema_version": (
            "dynamic_strategy_research_only_observation_report_dry_run_"
            "no_side_effect.v1"
        ),
        "status": "PASS",
        "report_mode": REPORT_MODE,
        "manual_report_dry_run_only": True,
        "observation_record_example_only": True,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    validation_errors: list[str],
    record_example: Mapping[str, Any],
    report_dry_run: Mapping[str, Any],
    evidence: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    dry_run = _as_mapping(sources.get("dry_run_result"))
    owner_decision = _as_mapping(sources.get("owner_decision"))
    log_schema_plan = _as_mapping(sources.get("log_schema_plan"))
    replay = _as_mapping(sources.get("replay_validation"))
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": list(SOURCE_TASKS),
        "source_artifacts": sources["source_artifacts"],
        "source_status": _source_status(sources),
        "source_ready_for_report_dry_run": ready,
        "source_validation_errors": validation_errors,
        "primary_observation_candidate": _primary_candidate(sources),
        "ranking_top_from_2365": dry_run.get("ranking_top_from_2365")
        or owner_decision.get("ranking_top_from_2365")
        or RANKING_TOP_FALLBACK,
        "robustness_top_from_2366": dry_run.get("robustness_top_from_2366")
        or owner_decision.get("robustness_top_from_2366")
        or PRIMARY_CANDIDATE_FALLBACK,
        "report_mode": REPORT_MODE,
        "observation_record_example_ready": ready,
        "observation_report_dry_run_ready": ready,
        "no_side_effect_evidence_ready": evidence.get("status") == "PASS",
        "owner_decision_from_2371": owner_decision.get("owner_decision"),
        "schema_ready_from_2372": log_schema_plan.get("observation_log_schema_ready"),
        "report_plan_ready_from_2372": log_schema_plan.get(
            "observation_report_plan_ready"
        ),
        "stable_semantic_replay_passed_from_2370": replay.get(
            "stable_semantic_replay_passed"
        ),
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _first_mapping(
            dry_run.get("requested_date_range"),
            replay.get("requested_date_range"),
            owner_decision.get("requested_date_range"),
            log_schema_plan.get("requested_date_range"),
        ),
        "data_quality": _first_mapping(
            dry_run.get("data_quality"),
            replay.get("data_quality"),
            owner_decision.get("data_quality"),
            log_schema_plan.get("data_quality"),
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_REPORT_DRY_RUN_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "manual_report_dry_run_only": True,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduler_attempted": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
        "periodic_daily_report_generated": False,
        "observation_record_example": dict(record_example),
        "observation_report_dry_run": dict(report_dry_run),
        "no_side_effect_evidence": dict(evidence),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "summary_findings": _summary_findings(ready=ready, sources=sources),
        "required_outputs_ready": _required_outputs_ready(ready),
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "json_path": str(output_root / "observation_report_dry_run_result.json"),
        "observation_record_example_json": str(
            output_root / "observation_record_example.json"
        ),
        "no_side_effect_evidence_json": str(
            output_root / "no_side_effect_evidence.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_research_only_observation_report_dry_run.md"
        ),
        "observation_record_example_markdown": str(
            docs_root
            / "dynamic_strategy_research_only_observation_record_example.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2374_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["observation_record_example_json"]),
        {
            "report_type": "dynamic_strategy_research_only_observation_record_example",
            "schema_version": "dynamic_strategy_research_only_observation_record.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "observation_record_example": payload["observation_record_example"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(artifact_paths["no_side_effect_evidence_json"]),
        {
            "report_type": (
                "dynamic_strategy_research_only_observation_report_dry_run_"
                "no_side_effect_evidence"
            ),
            "schema_version": (
                "dynamic_strategy_research_only_observation_report_dry_run_"
                "no_side_effect.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "no_side_effect_evidence": payload["no_side_effect_evidence"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["observation_record_example_markdown"]).write_text(
        _record_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    record = _as_mapping(payload.get("observation_record_example"))
    review = _as_mapping(record.get("review"))
    report = _as_mapping(payload.get("observation_report_dry_run"))
    return "\n".join(
        [
            "# 动态策略 research-only observation report dry-run",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- report mode：`{payload.get('report_mode')}`",
            (
                "- primary observation candidate："
                f"`{payload.get('primary_observation_candidate')}`"
            ),
            f"- observation decision：`{review.get('observation_decision')}`",
            f"- owner review required：`{review.get('owner_review_required')}`",
            f"- next route：`{payload.get('next_route')}`",
            "",
            "## Report sections",
            "",
            "\n".join(f"- {section}" for section in _as_list(report.get("sections"))),
            "",
            "## No-side-effect evidence",
            "",
            "- 是否写 event：否。",
            "- 是否 bind outcome：否。",
            "- 是否生成 daily report：否。",
            "- 是否启用 scheduler：否。",
            "- 是否创建 paper trade / shadow position：否。",
            "- 是否触发 production / broker：否。",
            "",
            "## Explicit non-goals",
            "",
            "- 不读取 fresh market data，不运行新 backtest，不重新计算 strategy state。",
            "- 不 append event，不 bind outcome，不 mutate outcome store。",
            "- 不启用 paper-shadow，不创建 paper trade 或 shadow position。",
            "- 不进入 production，不调用 broker，不发送 order。",
        ]
    )


def _record_markdown(payload: Mapping[str, Any]) -> str:
    record = _as_mapping(payload.get("observation_record_example"))
    lines = ["# 动态策略 research-only observation record example", ""]
    for section_name in (
        "identity",
        "candidate_context",
        "signal_context",
        "portfolio_preview",
        "cost_turnover",
        "comparison",
        "review",
        "guardrails",
    ):
        lines.append(f"## {section_name}")
        lines.append("")
        section = _as_mapping(record.get(section_name))
        for key, value in section.items():
            lines.append(f"- `{key}`：`{value}`")
        lines.append("")
    return "\n".join(lines)


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2374 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- next route：`{payload.get('next_route')}`",
            (
                "- route boundary：owner reassessment checkpoint；不是自动生成 "
                "TRADING-2375，不是 paper-shadow、scheduler、production 或 broker。"
            ),
        ]
    )


def _source_status(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "owner_decision": _as_mapping(sources.get("owner_decision")).get("status"),
        "log_schema_plan": _as_mapping(sources.get("log_schema_plan")).get("status"),
        "observation_dry_run_result": _as_mapping(
            sources.get("dry_run_result")
        ).get("status"),
        "observation_dry_run_record": _as_mapping(
            sources.get("dry_run_record_doc")
        ).get("status"),
        "replay_validation": _as_mapping(sources.get("replay_validation")).get(
            "status"
        ),
    }


def _summary_findings(
    *,
    ready: bool,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "report_dry_run_ready": ready,
        "primary_observation_candidate": _primary_candidate(sources),
        "manual_report_dry_run_only": True,
        "observation_record_example_ready": ready,
        "paper_shadow_remains_disabled": True,
        "event_outcome_mutation_remains_disabled": True,
        "daily_report_remains_disabled": True,
        "broker_path_remains_disabled": True,
        "next_route": NEXT_ROUTE,
    }


def _valid_until_window_state(signal_state: Mapping[str, Any]) -> str:
    raw_until = signal_state.get("advisory_valid_until")
    if not raw_until or raw_until == "NOT_COMPUTED_NO_FRESH_MARKET_DATA":
        return "NOT_COMPUTED_NO_FRESH_MARKET_DATA"
    return "CARRIED_FORWARD_FROM_PRIOR_ARTIFACT"


def _guardrails() -> dict[str, bool]:
    return {
        "research_only_observation": True,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
    }


def _side_effect_false_fields() -> tuple[str, ...]:
    return (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "event_append_enabled",
        "event_append_attempted",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "daily_report_generated",
    )


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "observation_report_dry_run": ready,
        "observation_record_example": ready,
        "no_side_effect_evidence": ready,
        "report_mode": ready,
        "paper_shadow_enabled_false": ready,
        "paper_trade_created_false": ready,
        "shadow_position_created_false": ready,
        "event_append_enabled_false": ready,
        "event_append_attempted_false": ready,
        "outcome_binding_enabled_false": ready,
        "outcome_binding_attempted_false": ready,
        "scheduler_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "daily_report_generated_false": ready,
        "recommended_next_research_task": ready,
    }


def _primary_candidate(sources: Mapping[str, Any]) -> str:
    dry_run = _as_mapping(sources.get("dry_run_result"))
    log_schema_plan = _as_mapping(sources.get("log_schema_plan"))
    owner_decision = _as_mapping(sources.get("owner_decision"))
    return str(
        dry_run.get("primary_observation_candidate")
        or log_schema_plan.get("primary_observation_candidate")
        or owner_decision.get("primary_observation_candidate")
        or PRIMARY_CANDIDATE_FALLBACK
    )


def _resolve_as_of(as_of_date: date | None, sources: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    for key in (
        "log_schema_plan",
        "owner_decision",
        "dry_run_result",
        "replay_validation",
    ):
        raw = _as_mapping(sources.get(key)).get("as_of")
        if isinstance(raw, str):
            try:
                return date.fromisoformat(raw[:10])
            except ValueError:
                continue
    return date.today()


def _source_artifact(path: Path, document: Any) -> dict[str, Any]:
    item = _as_mapping(document)
    return {
        "path": str(path),
        "sha256": _safe_sha256(path),
        "status": item.get("status"),
        "load_error": item.get("_load_error"),
    }


def _safe_sha256(path: Path) -> str | None:
    try:
        return _file_sha256(path)
    except OSError:
        return None


def _load_json_document(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"_load_error": str(exc)}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _first_mapping(*values: Any) -> Mapping[str, Any]:
    for value in values:
        item = _as_mapping(value)
        if item:
            return item
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
