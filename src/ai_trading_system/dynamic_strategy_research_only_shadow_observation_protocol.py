from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH as DEFAULT_2365_EVENT_RETEST_PATH,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    NEXT_ROUTE as SOURCE_2367_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    READY_STATUS as SOURCE_2367_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2368"
TASK_REGISTER_ID = (
    "TRADING-2368_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL"
)
REPORT_TYPE = "dynamic_strategy_research_only_shadow_observation_protocol"
SCHEMA_VERSION = "dynamic_strategy_research_only_shadow_observation_protocol.v1"
READY_STATUS = "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = "TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_Observation_Dry_Run"
PROTOCOL_MODE = "RESEARCH_ONLY"
OBSERVATION_FREQUENCY = "DAILY_OR_TRADING_DAY_IF_MANUALLY_RUN"
PRIMARY_CANDIDATE_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
STATIC_BASELINE_CANDIDATE_ID = "static_baseline"

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT
    / "owner_review_gate_result.json"
)
DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT
    / "candidate_owner_review_comparison.json"
)
DEFAULT_SOURCE_SHADOW_RESEARCH_GATE_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT
    / "shadow_research_gate_decision.json"
)
DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_matrix.json"
)
DEFAULT_SOURCE_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_research_only_shadow_observation_protocol(
    *,
    source_owner_review_gate_path: Path = DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    source_candidate_owner_review_comparison_path: Path = (
        DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH
    ),
    source_shadow_research_gate_decision_path: Path = (
        DEFAULT_SOURCE_SHADOW_RESEARCH_GATE_DECISION_PATH
    ),
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    source_sensitivity_matrix_path: Path = DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH,
    source_decision_update_path: Path = DEFAULT_SOURCE_DECISION_UPDATE_PATH,
    source_event_retest_path: Path = DEFAULT_2365_EVENT_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Path = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_owner_review_gate_path=source_owner_review_gate_path,
        source_candidate_owner_review_comparison_path=(
            source_candidate_owner_review_comparison_path
        ),
        source_shadow_research_gate_decision_path=(
            source_shadow_research_gate_decision_path
        ),
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_matrix_path=source_sensitivity_matrix_path,
        source_decision_update_path=source_decision_update_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
    )
    resolved_as_of = _resolve_as_of(as_of_date, sources)
    primary_candidate = _primary_observation_candidate(sources)
    if not bool(sources["ready_for_protocol"]):
        payload = _base_payload(
            status=BLOCKED_SOURCE_STATUS,
            as_of_date=resolved_as_of,
            sources=sources,
            primary_candidate=primary_candidate,
        )
        payload.update(
            {
                "research_only_shadow_observation_protocol_ready": False,
                "observation_field_schema_ready": False,
                "review_thresholds_ready": False,
                "research_only_shadow_observation_allowed": False,
                "observation_protocol": _blocked_protocol(primary_candidate, sources),
                "observation_field_schema": _observation_field_schema(),
                "review_thresholds": _review_thresholds(blocked=True),
                "summary_findings": _blocked_summary_findings(sources),
                "required_outputs_ready": _required_outputs_ready(False),
            }
        )
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    observation_protocol = _observation_protocol(sources, primary_candidate)
    field_schema = _observation_field_schema()
    review_thresholds = _review_thresholds(blocked=False)
    payload = _base_payload(
        status=READY_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        primary_candidate=primary_candidate,
    )
    payload.update(
        {
            "research_only_shadow_observation_protocol_ready": True,
            "observation_field_schema_ready": True,
            "review_thresholds_ready": True,
            "research_only_shadow_observation_allowed": True,
            "observation_protocol": observation_protocol,
            "observation_field_schema": field_schema,
            "review_thresholds": review_thresholds,
            "observation_cadence_plan": observation_protocol["observation_cadence_plan"],
            "comparison_plan": observation_protocol["comparison_plan"],
            "guardrail_plan": observation_protocol["guardrail_plan"],
            "recommended_next_research_task": NEXT_ROUTE,
            "summary_findings": _summary_findings(sources, primary_candidate),
            "required_outputs_ready": _required_outputs_ready(True),
        }
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    primary_candidate: str,
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": ["TRADING-2365", "TRADING-2366", "TRADING-2367"],
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_protocol": sources["ready_for_protocol"],
        "source_validation_errors": sources["source_validation_errors"],
        "primary_observation_candidate": primary_candidate,
        "ranking_top_from_2365": sources["ranking_top_from_2365"],
        "robustness_top_from_2366": sources["robustness_top_from_2366"],
        "ranking_robustness_divergence_detected": sources[
            "ranking_robustness_divergence_detected"
        ],
        "gate_decision_from_2367": sources["gate_decision_from_2367"],
        "primary_execution_cadence": sources["primary_execution_cadence"],
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _requested_date_range(sources),
        "data_quality": _source_data_quality(sources),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_PROTOCOL_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "manual_run_only": True,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_attempted": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduler_attempted": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "daily_report_generated": False,
        "next_route": NEXT_ROUTE,
    }


def _load_sources(
    *,
    source_owner_review_gate_path: Path,
    source_candidate_owner_review_comparison_path: Path,
    source_shadow_research_gate_decision_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_matrix_path: Path,
    source_decision_update_path: Path,
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_cadence_matrix_path: Path,
) -> dict[str, Any]:
    owner_gate = _load_json_document(source_owner_review_gate_path)
    owner_comparison = _load_json_document(source_candidate_owner_review_comparison_path)
    shadow_gate = _load_json_document(source_shadow_research_gate_decision_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_matrix = _load_json_document(source_sensitivity_matrix_path)
    decision_update = _load_json_document(source_decision_update_path)
    event_retest = _load_json_document(source_event_retest_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    cadence_matrix = _load_json_document(source_cadence_matrix_path)

    owner_gate_map = _as_mapping(owner_gate)
    shadow_gate_decision = _as_mapping(
        _first_present(
            _as_mapping(shadow_gate).get("shadow_research_gate_decision"),
            owner_gate_map.get("shadow_research_gate_decision"),
        )
    )
    decision_update_payload = _as_mapping(
        _first_present(
            _as_mapping(decision_update).get("decision_update"),
            _as_mapping(sensitivity_result).get("decision_update"),
        )
    )
    source_status = {
        "owner_review_gate": owner_gate_map.get("status"),
        "candidate_owner_review_comparison": _as_mapping(owner_comparison).get(
            "status"
        ),
        "shadow_research_gate_decision": _as_mapping(shadow_gate).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_matrix": _as_mapping(sensitivity_matrix).get("status"),
        "decision_update": _as_mapping(decision_update).get("status"),
        "event_retest": _as_mapping(event_retest).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "cadence_matrix": _as_mapping(cadence_matrix).get("status"),
    }
    ranking_top = str(
        _first_present(
            owner_gate_map.get("ranking_top_from_2365"),
            _as_mapping(event_retest)
            .get("summary", {})
            .get("top_candidate")
            if isinstance(_as_mapping(event_retest).get("summary"), Mapping)
            else None,
        )
        or ""
    )
    robustness_top = str(
        _first_present(
            owner_gate_map.get("robustness_top_from_2366"),
            decision_update_payload.get("top_candidate_after_sensitivity"),
            _as_mapping(sensitivity_result)
            .get("summary", {})
            .get("top_candidate_after_sensitivity")
            if isinstance(_as_mapping(sensitivity_result).get("summary"), Mapping)
            else None,
        )
        or ""
    )
    gate_candidate = str(
        _first_present(
            owner_gate_map.get("recommended_gate_candidate"),
            shadow_gate_decision.get("recommended_gate_candidate"),
            robustness_top,
            PRIMARY_CANDIDATE_FALLBACK,
        )
    )
    gate_decision = str(
        _first_present(
            owner_gate_map.get("recommended_gate_decision"),
            shadow_gate_decision.get("recommended_gate_decision"),
            "OWNER_REVIEW_REQUIRED",
        )
    )
    validation_errors = _source_validation_errors(
        source_status=source_status,
        owner_gate=owner_gate_map,
        owner_comparison=owner_comparison,
        shadow_gate_decision=shadow_gate_decision,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        gate_candidate=gate_candidate,
    )
    return {
        "owner_gate": owner_gate,
        "owner_comparison": owner_comparison,
        "shadow_gate": shadow_gate,
        "shadow_gate_decision": shadow_gate_decision,
        "sensitivity_result": sensitivity_result,
        "sensitivity_matrix": sensitivity_matrix,
        "decision_update": decision_update,
        "event_retest": event_retest,
        "candidate_ranking": candidate_ranking,
        "cadence_matrix": cadence_matrix,
        "candidate_review_comparison": _candidate_review_comparison(
            owner_gate, owner_comparison
        ),
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "recommended_gate_candidate": gate_candidate,
        "gate_decision_from_2367": gate_decision,
        "ranking_robustness_divergence_detected": bool(
            owner_gate_map.get("ranking_robustness_divergence_detected")
            if "ranking_robustness_divergence_detected" in owner_gate_map
            else ranking_top != robustness_top
        ),
        "primary_execution_cadence": str(
            _first_present(
                owner_gate_map.get("primary_execution_cadence"),
                _as_mapping(sensitivity_result).get("primary_execution_cadence"),
                _as_mapping(event_retest).get("primary_execution_cadence"),
                "valid_until_window",
            )
        ),
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "ready_for_protocol": not validation_errors,
        "source_artifacts": {
            "owner_review_gate": _source_artifact(
                source_owner_review_gate_path, owner_gate
            ),
            "candidate_owner_review_comparison": _source_artifact(
                source_candidate_owner_review_comparison_path, owner_comparison
            ),
            "shadow_research_gate_decision": _source_artifact(
                source_shadow_research_gate_decision_path, shadow_gate
            ),
            "sensitivity_result": _source_artifact(
                source_sensitivity_result_path, sensitivity_result
            ),
            "sensitivity_matrix": _source_artifact(
                source_sensitivity_matrix_path, sensitivity_matrix
            ),
            "decision_update": _source_artifact(
                source_decision_update_path, decision_update
            ),
            "event_retest": _source_artifact(source_event_retest_path, event_retest),
            "candidate_ranking": _source_artifact(
                source_candidate_ranking_path, candidate_ranking
            ),
            "cadence_matrix": _source_artifact(source_cadence_matrix_path, cadence_matrix),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    owner_gate: Mapping[str, Any],
    owner_comparison: Any,
    shadow_gate_decision: Mapping[str, Any],
    ranking_top: str,
    robustness_top: str,
    gate_candidate: str,
) -> list[str]:
    errors: list[str] = []
    for key in (
        "owner_review_gate",
        "candidate_owner_review_comparison",
        "shadow_research_gate_decision",
    ):
        if source_status.get(key) != SOURCE_2367_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("sensitivity_result", "sensitivity_matrix", "decision_update"):
        if source_status.get(key) != SOURCE_2366_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("event_retest", "candidate_ranking", "cadence_matrix"):
        if source_status.get(key) != SOURCE_2365_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    if owner_gate.get("next_route") not in {SOURCE_2367_EXPECTED_NEXT_ROUTE, None}:
        errors.append("owner_review_gate_next_route_not_trading_2368")
    if not bool(owner_gate.get("research_only_shadow_observation_allowed")):
        errors.append("research_only_shadow_observation_not_allowed_by_2367")
    if bool(owner_gate.get("paper_shadow_enabled")):
        errors.append("source_owner_gate_paper_shadow_enabled")
    if bool(owner_gate.get("production_enabled")):
        errors.append("source_owner_gate_production_enabled")
    if bool(owner_gate.get("broker_action_enabled")):
        errors.append("source_owner_gate_broker_action_enabled")
    if bool(shadow_gate_decision.get("paper_shadow_enabled")):
        errors.append("source_shadow_gate_paper_shadow_enabled")
    if not _candidate_review_comparison(owner_gate, owner_comparison):
        errors.append("candidate_review_comparison_missing")
    if not ranking_top:
        errors.append("ranking_top_from_2365_missing")
    if not robustness_top:
        errors.append("robustness_top_from_2366_missing")
    if not gate_candidate:
        errors.append("recommended_gate_candidate_missing")
    return errors


def _observation_protocol(
    sources: Mapping[str, Any],
    primary_candidate: str,
) -> dict[str, Any]:
    ranking_top = str(sources["ranking_top_from_2365"])
    comparison_candidates = _dedupe(
        [
            STATIC_BASELINE_CANDIDATE_ID,
            ranking_top,
            primary_candidate,
            str(sources["robustness_top_from_2366"]),
        ]
    )
    return {
        "schema_version": "dynamic_strategy_research_only_observation_protocol.v1",
        "mode": PROTOCOL_MODE,
        "protocol_scope": "REPORT_ONLY_SIGNAL_AND_PORTFOLIO_PREVIEW",
        "primary_candidate": primary_candidate,
        "candidate_source": "TRADING-2367",
        "gate_decision": sources["gate_decision_from_2367"],
        "baseline": STATIC_BASELINE_CANDIDATE_ID,
        "comparison_candidate": ranking_top,
        "comparison_candidates": comparison_candidates,
        "observation_frequency": OBSERVATION_FREQUENCY,
        "manual_run_only": True,
        "scheduler_enabled": False,
        "observation_cadence_plan": _observation_cadence_plan(),
        "observation_outputs": [
            "advisory_preview",
            "proposed_weight_delta",
            "valid_until_window",
            "risk_cap_state",
            "constraint_state",
            "no_trade_reason",
            "expected_turnover",
            "cooldown_state",
            "cost_assumption",
            "static_baseline_comparison",
            "ranking_top_comparison",
            "observation_review_flag",
        ],
        "comparison_plan": _comparison_plan(
            primary_candidate=primary_candidate,
            ranking_top=ranking_top,
            comparison_candidates=comparison_candidates,
        ),
        "guardrail_plan": _guardrail_plan(),
        "review_threshold_policy": (
            "QUALITATIVE_OWNER_REVIEW_TRIGGERS_ONLY_NO_NEW_NUMERIC_INVESTMENT_THRESHOLD"
        ),
        "next_route": NEXT_ROUTE,
    }


def _blocked_protocol(
    primary_candidate: str,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    protocol = _observation_protocol(sources, primary_candidate)
    protocol["mode"] = "BLOCKED_SOURCE_ARTIFACT_REVIEW_ONLY"
    protocol["observation_outputs"] = []
    protocol["source_validation_errors"] = sources["source_validation_errors"]
    return protocol


def _observation_cadence_plan() -> dict[str, Any]:
    return {
        "frequency": OBSERVATION_FREQUENCY,
        "trigger_path": "manual_research_cli_only",
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "run_boundary": (
            "Future 2369 dry-run may generate research-only observation artifacts; "
            "2368 only defines the protocol."
        ),
    }


def _comparison_plan(
    *,
    primary_candidate: str,
    ranking_top: str,
    comparison_candidates: Sequence[str],
) -> dict[str, Any]:
    return {
        "primary_candidate": primary_candidate,
        "static_baseline": STATIC_BASELINE_CANDIDATE_ID,
        "ranking_top_from_2365": ranking_top,
        "comparison_candidates": list(comparison_candidates),
        "comparison_metrics": [
            "advisory_preview_weight",
            "proposed_weight_delta",
            "dynamic_vs_static_preview_gap",
            "ranking_top_preview_gap",
            "expected_turnover",
            "cost_assumption",
            "cooldown_state",
            "constraint_state",
            "review_flag",
        ],
    }


def _guardrail_plan() -> dict[str, bool | str]:
    return {
        "research_only_shadow_observation": True,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "hard_fail_if_any_execution_flag_true": True,
        "boundary_note": (
            "Research-only observation records hypothetical previews in research "
            "artifacts only; it does not create executable paper-shadow state."
        ),
    }


def _observation_field_schema() -> dict[str, Any]:
    sections = {
        "identity": [
            "observation_id",
            "as_of",
            "source_task",
            "candidate_id",
            "candidate_version",
            "execution_cadence",
        ],
        "signal_state": [
            "signal_state",
            "advisory_valid_from",
            "advisory_valid_until",
            "signal_horizon",
            "signal_confidence_if_available",
        ],
        "portfolio_preview": [
            "current_reference_weight",
            "proposed_research_weight",
            "proposed_weight_delta",
            "max_single_step_weight_delta",
            "risk_cap_state",
            "constraint_state",
            "cooldown_state",
        ],
        "cost_and_turnover": [
            "expected_turnover",
            "transaction_cost_bps",
            "slippage_bps",
            "estimated_cost_drag",
            "turnover_cap_state",
        ],
        "comparison": [
            "static_baseline_weight",
            "static_baseline_expected_return_if_available",
            "ranking_top_candidate_weight",
            "robustness_top_candidate_weight",
            "dynamic_vs_static_preview_gap",
        ],
        "guardrails": [
            "research_only_shadow_observation",
            "paper_shadow_enabled",
            "event_append_enabled",
            "outcome_binding_enabled",
            "production_enabled",
            "broker_action_enabled",
        ],
        "review": [
            "owner_review_required",
            "review_reason",
            "escalation_flag",
            "observation_decision",
        ],
    }
    return {
        "schema_version": "dynamic_strategy_shadow_observation_field_schema.v1",
        "mode": PROTOCOL_MODE,
        "field_sections": [
            {
                "section": section,
                "fields": fields,
                "required": True,
                "production_effect": "none",
            }
            for section, fields in sections.items()
        ],
        "field_count": sum(len(fields) for fields in sections.values()),
        "append_event_allowed": False,
        "bind_outcome_allowed": False,
        "paper_trade_allowed": False,
        "broker_action_allowed": False,
    }


def _review_thresholds(*, blocked: bool) -> dict[str, Any]:
    triggers = [
        {
            "trigger_id": "drawdown_trigger",
            "condition": "candidate_drawdown_materially_worse_than_static_baseline",
            "action": "OWNER_REVIEW_REQUIRED",
        },
        {
            "trigger_id": "turnover_trigger",
            "condition": "expected_turnover_above_owner_accepted_threshold",
            "action": "OWNER_REVIEW_REQUIRED",
        },
        {
            "trigger_id": "cost_fragility_trigger",
            "condition": "edge_disappears_under_realistic_cost_assumptions",
            "action": "OWNER_REVIEW_REQUIRED",
        },
        {
            "trigger_id": "divergence_trigger",
            "condition": "ranking_top_and_robustness_top_disagree_repeatedly",
            "action": "OWNER_REVIEW_REQUIRED",
        },
        {
            "trigger_id": "stale_signal_trigger",
            "condition": "signal_executes_outside_valid_until_window",
            "action": "BLOCK_OBSERVATION_AND_REPORT",
        },
        {
            "trigger_id": "guardrail_trigger",
            "condition": "any_paper_shadow_production_or_broker_flag_true",
            "action": "HARD_FAIL",
        },
    ]
    return {
        "schema_version": "dynamic_strategy_shadow_observation_review_thresholds.v1",
        "mode": "BLOCKED_SOURCE_ARTIFACT_REVIEW_ONLY" if blocked else PROTOCOL_MODE,
        "threshold_policy": (
            "QUALITATIVE_OWNER_REVIEW_TRIGGERS_ONLY_NO_NEW_NUMERIC_INVESTMENT_THRESHOLD"
        ),
        "owner_review_triggers": triggers,
        "trigger_count": len(triggers),
        "hard_fail_conditions": [
            "paper_shadow_enabled_true",
            "paper_trade_created_true",
            "shadow_position_created_true",
            "event_append_enabled_true",
            "outcome_binding_enabled_true",
            "production_enabled_true",
            "broker_action_enabled_true",
        ],
    }


def _summary_findings(
    sources: Mapping[str, Any],
    primary_candidate: str,
) -> dict[str, Any]:
    return {
        "protocol_mode": PROTOCOL_MODE,
        "primary_observation_candidate": primary_candidate,
        "ranking_top_from_2365": sources["ranking_top_from_2365"],
        "robustness_top_from_2366": sources["robustness_top_from_2366"],
        "why_robustness_top_selected": (
            "TRADING-2367 preferred robustness top for research-only observation "
            "while retaining OWNER_REVIEW_REQUIRED."
        ),
        "paper_shadow_remains_disabled": True,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_remains_disabled": True,
        "outcome_binding_remains_disabled": True,
        "broker_path_remains_disabled": True,
        "owner_review_required_before_any_paper_shadow": True,
        "next_route": NEXT_ROUTE,
    }


def _blocked_summary_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "protocol_mode": "BLOCKED_SOURCE_ARTIFACT_REVIEW_ONLY",
        "source_validation_errors": sources["source_validation_errors"],
        "paper_shadow_remains_disabled": True,
        "broker_path_remains_disabled": True,
        "owner_review_required_before_any_paper_shadow": True,
        "next_route": NEXT_ROUTE,
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
        "json_path": str(output_root / "observation_protocol.json"),
        "observation_field_schema_json": str(
            output_root / "observation_field_schema.json"
        ),
        "review_thresholds_json": str(output_root / "review_thresholds.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_research_only_shadow_observation_protocol.md"
        ),
        "observation_field_schema_markdown": str(
            docs_root / "dynamic_strategy_shadow_observation_field_schema.md"
        ),
        "review_thresholds_markdown": str(
            docs_root / "dynamic_strategy_shadow_observation_review_thresholds.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2369_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["observation_field_schema_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_field_schema",
            "schema_version": "dynamic_strategy_shadow_observation_field_schema.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "observation_field_schema": payload["observation_field_schema"],
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    _write_json(
        Path(artifact_paths["review_thresholds_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_review_thresholds",
            "schema_version": "dynamic_strategy_shadow_observation_review_thresholds.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "review_thresholds": payload["review_thresholds"],
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["observation_field_schema_markdown"]).write_text(
        _field_schema_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["review_thresholds_markdown"]).write_text(
        _thresholds_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload), encoding="utf-8"
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("summary_findings"))
    return "\n".join(
        [
            "# 动态策略 research-only shadow observation protocol",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- protocol mode：`{summary.get('protocol_mode')}`",
            (
                "- primary observation candidate："
                f"`{payload.get('primary_observation_candidate')}`"
            ),
            f"- ranking top from 2365：`{payload.get('ranking_top_from_2365')}`",
            f"- robustness top from 2366：`{payload.get('robustness_top_from_2366')}`",
            f"- gate decision from 2367：`{payload.get('gate_decision_from_2367')}`",
            "- research-only shadow observation 和 paper-shadow execution 不同：",
            "  前者只写研究证据和 preview protocol；后者会创建模拟交易状态，本任务禁止。",
            "",
            "## 观察内容",
            "",
            (
                "- advisory preview、proposed weight delta、valid-until window、"
                "risk-cap state、constraint state、no-trade reason。"
            ),
            (
                "- expected turnover、cooldown state、cost assumption、"
                "static baseline comparison、2365 ranking top comparison。"
            ),
            "- owner review flag 和 escalation reason。",
            "",
            "## 明确不做",
            "",
            "- 不创建 paper trade 或 shadow position。",
            "- 不 append event、不 bind outcome、不 mutate outcome store。",
            "- 不启用 scheduler、不创建 scheduled task、不生成 daily report。",
            "- 不启用 production，不调用 broker，不发送订单。",
            "",
            "## Review thresholds",
            "",
            _threshold_table(payload),
            "",
            "## Recommended next route",
            "",
            f"- next route：`{payload.get('next_route')}`",
        ]
    )


def _field_schema_markdown(payload: Mapping[str, Any]) -> str:
    schema = _as_mapping(payload.get("observation_field_schema"))
    lines = ["# 动态策略 shadow observation field schema", ""]
    for section in _as_list(schema.get("field_sections")):
        item = _as_mapping(section)
        lines.append(f"## {item.get('section')}")
        lines.append("")
        for field in _as_list(item.get("fields")):
            lines.append(f"- `{field}`")
        lines.append("")
    lines.extend(
        [
            "## Guardrail",
            "",
            "- field schema 只供 future research-only dry-run 使用。",
            (
                "- schema 不允许 event append、outcome binding、paper trade、"
                "production 或 broker action。"
            ),
        ]
    )
    return "\n".join(lines)


def _thresholds_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 shadow observation review thresholds",
            "",
            _threshold_table(payload),
            "",
            "## Policy note",
            "",
            "- 当前只定义 qualitative owner-review triggers。",
            "- 未引入新的 numeric investment threshold。",
            "- 任何 paper-shadow、production 或 broker flag 变为 true 都必须 hard fail。",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2369 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- next route：`{payload.get('next_route')}`",
            (
                "- route boundary：research-only shadow observation dry-run；"
                "不是 paper-shadow execution、production 或 broker。"
            ),
        ]
    )


def _threshold_table(payload: Mapping[str, Any]) -> str:
    thresholds = _as_mapping(payload.get("review_thresholds"))
    lines = ["|trigger|condition|action|", "|---|---|---|"]
    for trigger in _as_list(thresholds.get("owner_review_triggers")):
        item = _as_mapping(trigger)
        lines.append(
            f"|`{item.get('trigger_id')}`|`{item.get('condition')}`|"
            f"`{item.get('action')}`|"
        )
    return "\n".join(lines)


def _primary_observation_candidate(sources: Mapping[str, Any]) -> str:
    return str(
        _first_present(
            sources.get("recommended_gate_candidate"),
            sources.get("robustness_top_from_2366"),
            PRIMARY_CANDIDATE_FALLBACK,
        )
    )


def _candidate_review_comparison(owner_gate: Any, owner_comparison: Any) -> list[Any]:
    return _as_list(
        _first_present(
            _as_mapping(owner_comparison).get("candidate_review_comparison"),
            _as_mapping(owner_gate).get("candidate_review_comparison"),
        )
    )


def _source_data_quality(sources: Mapping[str, Any]) -> dict[str, Any]:
    owner_quality = _as_mapping(_as_mapping(sources.get("owner_gate")).get("data_quality"))
    sensitivity_quality = _as_mapping(
        _as_mapping(sources.get("sensitivity_result")).get("data_quality")
    )
    event_quality = _as_mapping(
        _as_mapping(sources.get("event_retest")).get("data_quality")
    )
    status = (
        owner_quality.get("status")
        or sensitivity_quality.get("status")
        or event_quality.get("status")
        or "UNKNOWN"
    )
    return {
        "status": status,
        "source": "TRADING-2367",
        "carried_forward_from_prior_artifacts": True,
        "error_count": _first_number(
            owner_quality.get("error_count"),
            sensitivity_quality.get("error_count"),
            event_quality.get("error_count"),
        ),
        "warning_count": _first_number(
            owner_quality.get("warning_count"),
            sensitivity_quality.get("warning_count"),
            event_quality.get("warning_count"),
        ),
    }


def _requested_date_range(sources: Mapping[str, Any]) -> dict[str, Any]:
    for key in ("owner_gate", "sensitivity_result", "event_retest"):
        date_range = _as_mapping(_as_mapping(sources.get(key)).get("requested_date_range"))
        if date_range:
            return {
                "start": date_range.get("start") or date_range.get("start_date"),
                "end": date_range.get("end") or date_range.get("end_date"),
            }
    return {"start": None, "end": None}


def _resolve_as_of(as_of_date: date | None, sources: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    for key in ("owner_gate", "sensitivity_result", "event_retest"):
        raw = _as_mapping(sources.get(key)).get("as_of")
        if isinstance(raw, str):
            try:
                return date.fromisoformat(raw[:10])
            except ValueError:
                continue
    return date.today()


def _source_artifact(path: Path, document: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _safe_sha256(path),
        "status": _as_mapping(document).get("status"),
        "load_error": _as_mapping(document).get("_load_error"),
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


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "primary_observation_candidate": ready,
        "observation_protocol": ready,
        "observation_field_schema": ready,
        "review_thresholds": ready,
        "research_only_shadow_observation_allowed": ready,
        "paper_shadow_enabled_false": ready,
        "paper_trade_created_false": ready,
        "shadow_position_created_false": ready,
        "event_append_enabled_false": ready,
        "outcome_binding_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "recommended_next_research_task": ready,
    }


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return round(float(value), 6)
        except (TypeError, ValueError):
            continue
    return None


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value == "None" or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
