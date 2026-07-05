from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    DEFAULT_2365_EVENT_RETEST_PATH as DEFAULT_SOURCE_EVENT_RETEST_PATH,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT,
    DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH,
    DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    NEXT_ROUTE as SOURCE_2368_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    READY_STATUS as SOURCE_2368_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    READY_STATUS as SOURCE_2367_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2369"
TASK_REGISTER_ID = (
    "TRADING-2369_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN"
)
REPORT_TYPE = "dynamic_strategy_research_only_shadow_observation_dry_run"
SCHEMA_VERSION = "dynamic_strategy_research_only_shadow_observation_dry_run.v1"
READY_STATUS = "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_BLOCKED_SOURCE_ARTIFACT"
)
OBSERVATION_MODE = "RESEARCH_ONLY_DRY_RUN"
NEXT_ROUTE = (
    "TRADING-2370_Dynamic_Strategy_Research_Only_Shadow_Observation_"
    "Replay_No_Side_Effect_Validation"
)
PRIMARY_CANDIDATE_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
STATIC_BASELINE_CANDIDATE_ID = "static_baseline"
CURRENT_DYNAMIC_DEFAULT_ROLE = "current_dynamic_default"
DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
DECISION_BLOCK_OBSERVATION = "BLOCK_OBSERVATION_AND_REPORT"

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_OBSERVATION_PROTOCOL_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT
    / "observation_protocol.json"
)
DEFAULT_SOURCE_OBSERVATION_FIELD_SCHEMA_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT
    / "observation_field_schema.json"
)
DEFAULT_SOURCE_REVIEW_THRESHOLDS_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT
    / "review_thresholds.json"
)


def run_dynamic_strategy_research_only_shadow_observation_dry_run(
    *,
    source_observation_protocol_path: Path = DEFAULT_SOURCE_OBSERVATION_PROTOCOL_PATH,
    source_observation_field_schema_path: Path = (
        DEFAULT_SOURCE_OBSERVATION_FIELD_SCHEMA_PATH
    ),
    source_review_thresholds_path: Path = DEFAULT_SOURCE_REVIEW_THRESHOLDS_PATH,
    source_owner_review_gate_path: Path = DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    source_candidate_owner_review_comparison_path: Path = (
        DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH
    ),
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    source_event_retest_path: Path = DEFAULT_SOURCE_EVENT_RETEST_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_observation_protocol_path=source_observation_protocol_path,
        source_observation_field_schema_path=source_observation_field_schema_path,
        source_review_thresholds_path=source_review_thresholds_path,
        source_owner_review_gate_path=source_owner_review_gate_path,
        source_candidate_owner_review_comparison_path=(
            source_candidate_owner_review_comparison_path
        ),
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_event_retest_path=source_event_retest_path,
    )
    resolved_as_of = _resolve_as_of(as_of_date, sources)
    primary_candidate = _primary_candidate(sources)
    if not bool(sources["ready_for_dry_run"]):
        record = _blocked_observation_record(
            as_of_date=resolved_as_of,
            primary_candidate=primary_candidate,
            sources=sources,
        )
        evidence = _no_side_effect_evidence(record, blocked=True)
        payload = _base_payload(
            status=BLOCKED_SOURCE_STATUS,
            as_of_date=resolved_as_of,
            sources=sources,
            primary_candidate=primary_candidate,
            record=record,
            evidence=evidence,
        )
        payload.update(
            {
                "observation_decision": DECISION_BLOCK_OBSERVATION,
                "owner_review_required": True,
                "observation_dry_run_record_ready": False,
                "no_side_effect_evidence_ready": True,
                "summary_findings": _blocked_summary_findings(sources),
                "required_outputs_ready": _required_outputs_ready(False),
            }
        )
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    record = _observation_record(
        as_of_date=resolved_as_of,
        primary_candidate=primary_candidate,
        sources=sources,
    )
    evidence = _no_side_effect_evidence(record, blocked=False)
    payload = _base_payload(
        status=READY_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        primary_candidate=primary_candidate,
        record=record,
        evidence=evidence,
    )
    payload.update(
        {
            "observation_decision": record["review"]["observation_decision"],
            "owner_review_required": record["review"]["owner_review_required"],
            "review_reason": record["review"]["review_reason"],
            "escalation_flag": record["review"]["escalation_flag"],
            "observation_dry_run_record_ready": True,
            "no_side_effect_evidence_ready": True,
            "recommended_next_research_task": NEXT_ROUTE,
            "summary_findings": _summary_findings(sources, record),
            "required_outputs_ready": _required_outputs_ready(True),
        }
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_observation_protocol_path: Path,
    source_observation_field_schema_path: Path,
    source_review_thresholds_path: Path,
    source_owner_review_gate_path: Path,
    source_candidate_owner_review_comparison_path: Path,
    source_sensitivity_result_path: Path,
    source_event_retest_path: Path,
) -> dict[str, Any]:
    protocol = _load_json_document(source_observation_protocol_path)
    field_schema = _load_json_document(source_observation_field_schema_path)
    review_thresholds = _load_json_document(source_review_thresholds_path)
    owner_gate = _load_json_document(source_owner_review_gate_path)
    owner_comparison = _load_json_document(source_candidate_owner_review_comparison_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    event_retest = _load_json_document(source_event_retest_path)

    protocol_map = _as_mapping(protocol)
    protocol_body = _as_mapping(protocol_map.get("observation_protocol"))
    source_status = {
        "observation_protocol": protocol_map.get("status"),
        "observation_field_schema": _as_mapping(field_schema).get("status"),
        "review_thresholds": _as_mapping(review_thresholds).get("status"),
        "owner_review_gate": _as_mapping(owner_gate).get("status"),
        "candidate_owner_review_comparison": _as_mapping(owner_comparison).get(
            "status"
        ),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "event_retest": _as_mapping(event_retest).get("status"),
    }
    ranking_top = str(
        _first_present(
            protocol_map.get("ranking_top_from_2365"),
            protocol_body.get("comparison_candidate"),
            _as_mapping(owner_gate).get("ranking_top_from_2365"),
            RANKING_TOP_FALLBACK,
        )
    )
    robustness_top = str(
        _first_present(
            protocol_map.get("robustness_top_from_2366"),
            _as_mapping(owner_gate).get("robustness_top_from_2366"),
            _as_mapping(_as_mapping(sensitivity_result).get("summary")).get(
                "top_candidate_after_sensitivity"
            ),
            PRIMARY_CANDIDATE_FALLBACK,
        )
    )
    primary_candidate = str(
        _first_present(
            protocol_map.get("primary_observation_candidate"),
            protocol_body.get("primary_candidate"),
            _as_mapping(owner_gate).get("recommended_gate_candidate"),
            robustness_top,
        )
    )
    comparison_rows = _candidate_review_comparison(owner_gate, owner_comparison)
    comparison_candidates = _comparison_candidates(
        protocol_body=protocol_body,
        comparison_rows=comparison_rows,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    validation_errors = _source_validation_errors(
        source_status=source_status,
        protocol=protocol_map,
        field_schema=field_schema,
        review_thresholds=review_thresholds,
        owner_gate=owner_gate,
        owner_comparison=owner_comparison,
        sensitivity_result=sensitivity_result,
        event_retest=event_retest,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        comparison_rows=comparison_rows,
    )
    return {
        "observation_protocol": protocol,
        "observation_field_schema": field_schema,
        "review_thresholds": review_thresholds,
        "owner_gate": owner_gate,
        "owner_comparison": owner_comparison,
        "sensitivity_result": sensitivity_result,
        "event_retest": event_retest,
        "candidate_review_comparison": comparison_rows,
        "comparison_candidates": comparison_candidates,
        "primary_observation_candidate": primary_candidate,
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "gate_decision_from_2367": str(
            _first_present(
                protocol_map.get("gate_decision_from_2367"),
                protocol_body.get("gate_decision"),
                _as_mapping(owner_gate).get("recommended_gate_decision"),
                DECISION_OWNER_REVIEW,
            )
        ),
        "primary_execution_cadence": str(
            _first_present(
                protocol_map.get("primary_execution_cadence"),
                _as_mapping(owner_gate).get("primary_execution_cadence"),
                _as_mapping(sensitivity_result).get("primary_execution_cadence"),
                "valid_until_window",
            )
        ),
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "ready_for_dry_run": not validation_errors,
        "source_artifacts": {
            "observation_protocol": _source_artifact(
                source_observation_protocol_path, protocol
            ),
            "observation_field_schema": _source_artifact(
                source_observation_field_schema_path, field_schema
            ),
            "review_thresholds": _source_artifact(
                source_review_thresholds_path, review_thresholds
            ),
            "owner_review_gate": _source_artifact(
                source_owner_review_gate_path, owner_gate
            ),
            "candidate_owner_review_comparison": _source_artifact(
                source_candidate_owner_review_comparison_path, owner_comparison
            ),
            "sensitivity_result": _source_artifact(
                source_sensitivity_result_path, sensitivity_result
            ),
            "event_retest": _source_artifact(source_event_retest_path, event_retest),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    protocol: Mapping[str, Any],
    field_schema: Any,
    review_thresholds: Any,
    owner_gate: Any,
    owner_comparison: Any,
    sensitivity_result: Any,
    event_retest: Any,
    primary_candidate: str,
    ranking_top: str,
    robustness_top: str,
    comparison_rows: Sequence[Any],
) -> list[str]:
    errors: list[str] = []
    for key in ("observation_protocol", "observation_field_schema", "review_thresholds"):
        if source_status.get(key) != SOURCE_2368_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    if source_status.get("owner_review_gate") != SOURCE_2367_READY_STATUS:
        errors.append("owner_review_gate_status_not_ready")
    if source_status.get("candidate_owner_review_comparison") != SOURCE_2367_READY_STATUS:
        errors.append("candidate_owner_review_comparison_status_not_ready")
    if source_status.get("sensitivity_result") != SOURCE_2366_READY_STATUS:
        errors.append("sensitivity_result_status_not_ready")
    if source_status.get("event_retest") != SOURCE_2365_READY_STATUS:
        errors.append("event_retest_status_not_ready")

    protocol_body = _as_mapping(protocol.get("observation_protocol"))
    if protocol.get("next_route") != SOURCE_2368_EXPECTED_NEXT_ROUTE:
        errors.append("observation_protocol_next_route_not_trading_2369")
    if protocol_body.get("mode") != "RESEARCH_ONLY":
        errors.append("observation_protocol_mode_not_research_only")
    if not bool(protocol.get("research_only_shadow_observation_allowed")):
        errors.append("observation_protocol_research_only_not_allowed")
    if not bool(protocol.get("observation_field_schema_ready")):
        errors.append("observation_field_schema_not_ready_in_protocol")
    if not bool(protocol.get("review_thresholds_ready")):
        errors.append("review_thresholds_not_ready_in_protocol")
    if bool(protocol.get("paper_shadow_enabled")):
        errors.append("source_protocol_paper_shadow_enabled")
    if bool(protocol.get("event_append_enabled")):
        errors.append("source_protocol_event_append_enabled")
    if bool(protocol.get("outcome_binding_enabled")):
        errors.append("source_protocol_outcome_binding_enabled")
    if bool(protocol.get("production_enabled")):
        errors.append("source_protocol_production_enabled")
    if bool(protocol.get("broker_action_enabled")):
        errors.append("source_protocol_broker_action_enabled")

    field_schema_body = _as_mapping(_as_mapping(field_schema).get("observation_field_schema"))
    if bool(field_schema_body.get("append_event_allowed")):
        errors.append("field_schema_append_event_allowed")
    if bool(field_schema_body.get("bind_outcome_allowed")):
        errors.append("field_schema_bind_outcome_allowed")
    if bool(field_schema_body.get("paper_trade_allowed")):
        errors.append("field_schema_paper_trade_allowed")
    if bool(field_schema_body.get("broker_action_allowed")):
        errors.append("field_schema_broker_action_allowed")
    if not _as_list(_as_mapping(review_thresholds).get("review_thresholds", {})):
        threshold_body = _as_mapping(_as_mapping(review_thresholds).get("review_thresholds"))
        if not _as_list(threshold_body.get("owner_review_triggers")):
            errors.append("review_threshold_triggers_missing")

    owner_gate_map = _as_mapping(owner_gate)
    if not bool(owner_gate_map.get("research_only_shadow_observation_allowed")):
        errors.append("owner_gate_research_only_not_allowed")
    if bool(owner_gate_map.get("paper_shadow_enabled")):
        errors.append("owner_gate_paper_shadow_enabled")
    if bool(owner_gate_map.get("event_append_enabled")):
        errors.append("owner_gate_event_append_enabled")
    if bool(owner_gate_map.get("outcome_binding_enabled")):
        errors.append("owner_gate_outcome_binding_enabled")
    if bool(owner_gate_map.get("production_enabled")):
        errors.append("owner_gate_production_enabled")
    if bool(owner_gate_map.get("broker_action_enabled")):
        errors.append("owner_gate_broker_action_enabled")

    if bool(_as_mapping(sensitivity_result).get("paper_shadow_allowed")):
        errors.append("sensitivity_result_paper_shadow_allowed")
    if bool(_as_mapping(event_retest).get("paper_shadow_allowed")):
        errors.append("event_retest_paper_shadow_allowed")
    if not primary_candidate:
        errors.append("primary_observation_candidate_missing")
    if not ranking_top:
        errors.append("ranking_top_from_2365_missing")
    if not robustness_top:
        errors.append("robustness_top_from_2366_missing")
    if primary_candidate != robustness_top:
        errors.append("primary_candidate_not_robustness_top")
    if not comparison_rows:
        errors.append("candidate_review_comparison_missing")
    if not _find_candidate_row(comparison_rows, primary_candidate):
        errors.append("primary_candidate_comparison_missing")
    if not _find_candidate_row(comparison_rows, ranking_top):
        errors.append("ranking_top_comparison_missing")
    if not _find_candidate_row(comparison_rows, STATIC_BASELINE_CANDIDATE_ID):
        errors.append("static_baseline_comparison_missing")
    if not _as_mapping(owner_comparison).get("candidate_review_comparison"):
        errors.append("candidate_owner_review_comparison_rows_missing")
    return errors


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    primary_candidate: str,
    record: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": ["TRADING-2365", "TRADING-2366", "TRADING-2367", "TRADING-2368"],
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_dry_run": sources["ready_for_dry_run"],
        "source_validation_errors": sources["source_validation_errors"],
        "observation_mode": OBSERVATION_MODE,
        "primary_observation_candidate": primary_candidate,
        "ranking_top_from_2365": sources["ranking_top_from_2365"],
        "robustness_top_from_2366": sources["robustness_top_from_2366"],
        "execution_cadence": sources["primary_execution_cadence"],
        "gate_decision_from_2367": sources["gate_decision_from_2367"],
        "observation_protocol_loaded": (
            sources["source_status"].get("observation_protocol")
            == SOURCE_2368_READY_STATUS
        ),
        "observation_field_schema_loaded": (
            sources["source_status"].get("observation_field_schema")
            == SOURCE_2368_READY_STATUS
        ),
        "review_thresholds_loaded": (
            sources["source_status"].get("review_thresholds") == SOURCE_2368_READY_STATUS
        ),
        "owner_review_gate_loaded": (
            sources["source_status"].get("owner_review_gate") == SOURCE_2367_READY_STATUS
        ),
        "sensitivity_result_loaded": (
            sources["source_status"].get("sensitivity_result") == SOURCE_2366_READY_STATUS
        ),
        "event_retest_loaded": (
            sources["source_status"].get("event_retest") == SOURCE_2365_READY_STATUS
        ),
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _requested_date_range(sources),
        "data_quality": _source_data_quality(sources),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_DRY_RUN_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "dry_run_only": True,
        "manual_run_only": True,
        "manual_review_required": True,
        "research_only_shadow_observation_allowed": True,
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
        "observation_dry_run_record": record,
        "no_side_effect_evidence": evidence,
        "next_route": NEXT_ROUTE,
    }


def _observation_record(
    *,
    as_of_date: date,
    primary_candidate: str,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _as_list(sources.get("candidate_review_comparison"))
    primary = _as_mapping(_find_candidate_row(rows, primary_candidate))
    ranking = _as_mapping(_find_candidate_row(rows, sources["ranking_top_from_2365"]))
    robustness = _as_mapping(_find_candidate_row(rows, sources["robustness_top_from_2366"]))
    static = _as_mapping(_find_candidate_row(rows, STATIC_BASELINE_CANDIDATE_ID))
    current_dynamic_default = _current_dynamic_default_row(rows)
    review = _review_section(primary=primary, sources=sources)
    return {
        "schema_version": "dynamic_strategy_shadow_observation_dry_run_record.v1",
        "observation_id": _observation_id(as_of_date, primary_candidate),
        "observation_mode": OBSERVATION_MODE,
        "identity": {
            "observation_id": _observation_id(as_of_date, primary_candidate),
            "as_of": as_of_date.isoformat(),
            "source_task": TASK_ID,
            "candidate_id": primary_candidate,
            "candidate_version": "source_artifact_candidate_from_trading_2367",
            "execution_cadence": sources["primary_execution_cadence"],
        },
        "signal_state": {
            "signal_state": "SOURCE_ARTIFACT_PREVIEW_ONLY_NOT_RECOMPUTED",
            "advisory_valid_from": as_of_date.isoformat(),
            "advisory_valid_until": "NOT_COMPUTED_NO_FRESH_MARKET_DATA",
            "signal_horizon": sources["primary_execution_cadence"],
            "signal_confidence_if_available": None,
        },
        "portfolio_preview": {
            "reference_weight": None,
            "proposed_research_weight": None,
            "proposed_weight_delta": None,
            "max_single_step_weight_delta": None,
            "risk_cap_state": "NOT_RECOMPUTED_PRIOR_ARTIFACT_DRY_RUN",
            "constraint_state": _constraint_state(primary),
            "cooldown_state": primary.get("cooldown_fragility"),
            "no_trade_reason": "RESEARCH_ONLY_DRY_RUN_NO_EXECUTION",
        },
        "cost_and_turnover": {
            "expected_turnover": _number_or_none(primary.get("turnover")),
            "transaction_cost_bps": _drag_to_bps(primary.get("transaction_cost_drag")),
            "slippage_bps": _drag_to_bps(primary.get("slippage_drag")),
            "estimated_cost_drag": _estimated_cost_drag(primary),
            "turnover_cap_state": _turnover_cap_state(primary),
        },
        "comparison": {
            "static_baseline_comparison": _comparison_summary(static),
            "ranking_top_candidate_comparison": _comparison_summary(ranking),
            "robustness_top_candidate_comparison": _comparison_summary(robustness),
            "current_dynamic_default_if_available": _comparison_summary(
                _as_mapping(current_dynamic_default)
            )
            if current_dynamic_default
            else None,
            "dynamic_vs_static_preview_gap": _number_or_none(
                primary.get("dynamic_vs_static_gap")
            ),
            "comparison_candidates": sources["comparison_candidates"],
        },
        "review": review,
        "guardrails": _guardrail_fields(),
    }


def _blocked_observation_record(
    *,
    as_of_date: date,
    primary_candidate: str,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    record = _observation_record(
        as_of_date=as_of_date,
        primary_candidate=primary_candidate,
        sources=sources,
    )
    record["observation_mode"] = "BLOCKED_SOURCE_ARTIFACT_REVIEW_ONLY"
    record["review"] = {
        "owner_review_required": True,
        "review_reason": "source_validation_errors_present",
        "escalation_flag": DECISION_BLOCK_OBSERVATION,
        "observation_decision": DECISION_BLOCK_OBSERVATION,
        "source_validation_errors": sources["source_validation_errors"],
    }
    return record


def _review_section(
    *,
    primary: Mapping[str, Any],
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    if sources.get("gate_decision_from_2367") == DECISION_OWNER_REVIEW:
        reasons.append("TRADING-2367 gate decision remains OWNER_REVIEW_REQUIRED")
    if primary.get("turnover_acceptable_after_2366") is False:
        reasons.append("turnover requires owner review after TRADING-2366")
    if bool(sources.get("ranking_top_from_2365")) and bool(
        sources.get("robustness_top_from_2366")
    ) and sources["ranking_top_from_2365"] != sources["robustness_top_from_2366"]:
        reasons.append("ranking top and robustness top diverge")
    if not reasons:
        return {
            "owner_review_required": False,
            "review_reason": "no qualitative trigger in prior artifacts",
            "escalation_flag": "NONE",
            "observation_decision": "OBSERVE_ONLY_CONTINUE",
        }
    return {
        "owner_review_required": True,
        "review_reason": "; ".join(reasons),
        "escalation_flag": DECISION_OWNER_REVIEW,
        "observation_decision": DECISION_OWNER_REVIEW,
    }


def _no_side_effect_evidence(
    record: Mapping[str, Any],
    *,
    blocked: bool,
) -> dict[str, Any]:
    safety = _guardrail_fields()
    return {
        "schema_version": "dynamic_strategy_shadow_observation_no_side_effect.v1",
        "status": "BLOCKED_SOURCE_ARTIFACT_REVIEW_ONLY" if blocked else "PASS",
        "observation_id": record.get("observation_id"),
        "dry_run_only": True,
        "research_artifact_written": True,
        "event_append_attempted": False,
        "event_append_performed": False,
        "historical_event_log_mutated": False,
        "outcome_binding_attempted": False,
        "outcome_bound": False,
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
        "guardrail_fields": safety,
        "hard_fail_if_any_execution_flag_true": True,
    }


def _guardrail_fields() -> dict[str, bool | str]:
    return {
        "research_only_shadow_observation": True,
        "observation_mode": OBSERVATION_MODE,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "daily_report_generated": False,
    }


def _summary_findings(
    sources: Mapping[str, Any],
    record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "protocol_mode": OBSERVATION_MODE,
        "primary_observation_candidate": sources["primary_observation_candidate"],
        "why_robustness_top_selected": (
            "TRADING-2367 selected the robustness top for research-only "
            "observation while retaining owner review."
        ),
        "observation_decision": _as_mapping(record.get("review")).get(
            "observation_decision"
        ),
        "paper_shadow_remains_disabled": True,
        "event_outcome_mutation_remains_disabled": True,
        "broker_path_remains_disabled": True,
        "next_route": NEXT_ROUTE,
    }


def _blocked_summary_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "protocol_mode": "BLOCKED_SOURCE_ARTIFACT_REVIEW_ONLY",
        "source_validation_errors": sources["source_validation_errors"],
        "paper_shadow_remains_disabled": True,
        "event_outcome_mutation_remains_disabled": True,
        "broker_path_remains_disabled": True,
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
        "json_path": str(output_root / "observation_dry_run_result.json"),
        "observation_dry_run_record_json": str(
            output_root / "observation_dry_run_record.json"
        ),
        "no_side_effect_evidence_json": str(
            output_root / "no_side_effect_evidence.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_research_only_shadow_observation_dry_run.md"
        ),
        "observation_dry_run_record_markdown": str(
            docs_root / "dynamic_strategy_shadow_observation_dry_run_record.md"
        ),
        "no_side_effect_evidence_markdown": str(
            docs_root / "dynamic_strategy_shadow_observation_no_side_effect_evidence.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2370_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["observation_dry_run_record_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_dry_run_record",
            "schema_version": "dynamic_strategy_shadow_observation_dry_run_record.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "observation_mode": payload["observation_mode"],
            "observation_dry_run_record": payload["observation_dry_run_record"],
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    _write_json(
        Path(artifact_paths["no_side_effect_evidence_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_no_side_effect_evidence",
            "schema_version": (
                "dynamic_strategy_shadow_observation_no_side_effect.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "no_side_effect_evidence": payload["no_side_effect_evidence"],
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["observation_dry_run_record_markdown"]).write_text(
        _record_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["no_side_effect_evidence_markdown"]).write_text(
        _evidence_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload), encoding="utf-8"
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    record = _as_mapping(payload.get("observation_dry_run_record"))
    review = _as_mapping(record.get("review"))
    comparison = _as_mapping(record.get("comparison"))
    return "\n".join(
        [
            "# 动态策略 research-only shadow observation dry-run",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- observation mode：`{payload.get('observation_mode')}`",
            (
                "- primary observation candidate："
                f"`{payload.get('primary_observation_candidate')}`"
            ),
            f"- ranking top from 2365：`{payload.get('ranking_top_from_2365')}`",
            f"- robustness top from 2366：`{payload.get('robustness_top_from_2366')}`",
            f"- observation decision：`{payload.get('observation_decision')}`",
            f"- owner review required：`{payload.get('owner_review_required')}`",
            "",
            "## Source protocol from TRADING-2368",
            "",
            f"- protocol loaded：`{payload.get('observation_protocol_loaded')}`",
            f"- field schema loaded：`{payload.get('observation_field_schema_loaded')}`",
            f"- review thresholds loaded：`{payload.get('review_thresholds_loaded')}`",
            "",
            "## Observation candidate",
            "",
            "- 本次 dry-run 观察 `dynamic_regime_overlay_v0_4_lower_turnover`。",
            (
                "- 观察 robustness top 的原因：TRADING-2367 在收益排名 top 和"
                "稳健性 top 分歧后推荐 robustness top 做 research-only 观察。"
            ),
            "",
            "## Observation dry-run record",
            "",
            f"- observation id：`{record.get('observation_id')}`",
            f"- signal state：`{_as_mapping(record.get('signal_state')).get('signal_state')}`",
            (
                "- no trade reason："
                f"`{_as_mapping(record.get('portfolio_preview')).get('no_trade_reason')}`"
            ),
            "",
            "## Static baseline comparison",
            "",
            _comparison_lines(_as_mapping(comparison.get("static_baseline_comparison"))),
            "",
            "## Ranking top vs robustness top comparison",
            "",
            _comparison_lines(
                _as_mapping(comparison.get("ranking_top_candidate_comparison"))
            ),
            _comparison_lines(
                _as_mapping(comparison.get("robustness_top_candidate_comparison"))
            ),
            "",
            "## Review flags and thresholds",
            "",
            f"- review reason：`{review.get('review_reason')}`",
            f"- escalation flag：`{review.get('escalation_flag')}`",
            "",
            "## No-side-effect evidence",
            "",
            "- 是否生成 paper trade：否。",
            "- 是否创建 shadow position：否。",
            "- 是否写 event：否。",
            "- 是否 bind outcome：否。",
            "- 是否生成 daily report：否。",
            "- 是否触发 production / broker：否。",
            "",
            "## Explicit non-goals",
            "",
            "- 不启用 scheduler，不创建 scheduled task。",
            "- 不 append event，不 bind outcome，不 mutate outcome store。",
            "- 不启用 paper-shadow，不创建 paper trade 或 shadow position。",
            "- 不进入 production，不调用 broker，不发送 order。",
            "",
            "## Recommended next route",
            "",
            f"- next route：`{payload.get('next_route')}`",
        ]
    )


def _record_markdown(payload: Mapping[str, Any]) -> str:
    record = _as_mapping(payload.get("observation_dry_run_record"))
    lines = ["# 动态策略 shadow observation dry-run record", ""]
    for section_name in (
        "identity",
        "signal_state",
        "portfolio_preview",
        "cost_and_turnover",
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


def _evidence_markdown(payload: Mapping[str, Any]) -> str:
    evidence = _as_mapping(payload.get("no_side_effect_evidence"))
    return "\n".join(
        [
            "# 动态策略 shadow observation no-side-effect evidence",
            "",
            f"- status：`{evidence.get('status')}`",
            f"- dry_run_only：`{evidence.get('dry_run_only')}`",
            f"- event_append_attempted：`{evidence.get('event_append_attempted')}`",
            f"- event_append_performed：`{evidence.get('event_append_performed')}`",
            f"- outcome_binding_attempted：`{evidence.get('outcome_binding_attempted')}`",
            f"- outcome_store_mutated：`{evidence.get('outcome_store_mutated')}`",
            f"- paper_trade_created：`{evidence.get('paper_trade_created')}`",
            f"- shadow_position_created：`{evidence.get('shadow_position_created')}`",
            f"- scheduler_enabled：`{evidence.get('scheduler_enabled')}`",
            f"- production_enabled：`{evidence.get('production_enabled')}`",
            f"- broker_action_enabled：`{evidence.get('broker_action_enabled')}`",
            f"- daily_report_generated：`{evidence.get('daily_report_generated')}`",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2370 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- next route：`{payload.get('next_route')}`",
            (
                "- route boundary：research-only shadow observation replay "
                "no-side-effect validation；不是 paper-shadow execution、production 或 broker。"
            ),
        ]
    )


def _comparison_lines(row: Mapping[str, Any]) -> str:
    if not row:
        return "- comparison row：`MISSING`"
    return (
        f"- `{row.get('candidate_id')}`：decision=`{row.get('decision')}`，"
        f"dynamic_vs_static_gap=`{row.get('dynamic_vs_static_gap')}`，"
        f"max_drawdown=`{row.get('max_drawdown')}`，"
        f"turnover=`{row.get('turnover')}`。"
    )


def _candidate_review_comparison(owner_gate: Any, owner_comparison: Any) -> list[Any]:
    return _as_list(
        _first_present(
            _as_mapping(owner_comparison).get("candidate_review_comparison"),
            _as_mapping(owner_gate).get("candidate_review_comparison"),
        )
    )


def _comparison_candidates(
    *,
    protocol_body: Mapping[str, Any],
    comparison_rows: Sequence[Any],
    primary_candidate: str,
    ranking_top: str,
    robustness_top: str,
) -> list[str]:
    current_default = _current_dynamic_default_row(comparison_rows)
    return _dedupe(
        [
            STATIC_BASELINE_CANDIDATE_ID,
            ranking_top,
            robustness_top,
            primary_candidate,
            str(_as_mapping(current_default).get("candidate_id") or ""),
            *[str(item) for item in _as_list(protocol_body.get("comparison_candidates"))],
        ]
    )


def _comparison_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "roles": row.get("roles"),
        "decision": _first_present(
            row.get("decision_after_2366"),
            row.get("recommended_gate_decision"),
            row.get("decision_from_2365"),
        ),
        "cost_adjusted_return": _number_or_none(row.get("cost_adjusted_return")),
        "dynamic_vs_static_gap": _number_or_none(row.get("dynamic_vs_static_gap")),
        "max_drawdown": _number_or_none(row.get("max_drawdown")),
        "turnover": _number_or_none(row.get("turnover")),
        "turnover_acceptable_after_2366": row.get("turnover_acceptable_after_2366"),
        "cooldown_fragility": row.get("cooldown_fragility"),
        "review_priority_rank": _number_or_none(row.get("review_priority_rank")),
    }


def _find_candidate_row(rows: Sequence[Any], candidate_id: str) -> Mapping[str, Any]:
    for row in rows:
        item = _as_mapping(row)
        if item.get("candidate_id") == candidate_id:
            return item
    return {}


def _current_dynamic_default_row(rows: Sequence[Any]) -> Mapping[str, Any]:
    for row in rows:
        item = _as_mapping(row)
        roles = {str(role) for role in _as_list(item.get("roles"))}
        if CURRENT_DYNAMIC_DEFAULT_ROLE in roles:
            return item
    return {}


def _constraint_state(row: Mapping[str, Any]) -> str:
    constraint_hits = _number_or_none(row.get("constraint_hit_count"))
    stale_hits = _number_or_none(row.get("stale_signal_count"))
    if constraint_hits == 0 and stale_hits == 0:
        return "NO_SOURCE_CONSTRAINT_OR_STALE_SIGNAL_HITS_IN_PRIOR_ARTIFACT"
    return "SOURCE_ARTIFACT_CONSTRAINT_OR_STALE_SIGNAL_REVIEW_REQUIRED"


def _turnover_cap_state(row: Mapping[str, Any]) -> str:
    if row.get("turnover_acceptable_after_2366") is False:
        return "OWNER_REVIEW_REQUIRED_TURNOVER_NOT_ACCEPTABLE_AFTER_2366"
    if row.get("turnover_acceptable_after_2366") is True:
        return "ACCEPTABLE_IN_PRIOR_ARTIFACT"
    return "NOT_EVALUATED_IN_PRIOR_ARTIFACT"


def _estimated_cost_drag(row: Mapping[str, Any]) -> float | None:
    values = [
        _number_or_none(row.get("transaction_cost_drag")),
        _number_or_none(row.get("slippage_drag")),
    ]
    if any(value is None for value in values):
        return None
    return round(sum(value for value in values if value is not None), 6)


def _drag_to_bps(value: Any) -> float | None:
    number = _number_or_none(value)
    if number is None:
        return None
    return round(number * 10000, 3)


def _observation_id(as_of_date: date, primary_candidate: str) -> str:
    return f"TRADING-2369_{as_of_date.isoformat()}_{primary_candidate}"


def _primary_candidate(sources: Mapping[str, Any]) -> str:
    return str(
        _first_present(
            sources.get("primary_observation_candidate"),
            sources.get("robustness_top_from_2366"),
            PRIMARY_CANDIDATE_FALLBACK,
        )
    )


def _source_data_quality(sources: Mapping[str, Any]) -> dict[str, Any]:
    for key, source_name in (
        ("observation_protocol", "TRADING-2368"),
        ("owner_gate", "TRADING-2367"),
        ("sensitivity_result", "TRADING-2366"),
        ("event_retest", "TRADING-2365"),
    ):
        quality = _as_mapping(_as_mapping(sources.get(key)).get("data_quality"))
        if quality:
            return {
                "status": quality.get("status") or "UNKNOWN",
                "source": source_name,
                "carried_forward_from_prior_artifacts": True,
                "error_count": _number_or_none(quality.get("error_count")),
                "warning_count": _number_or_none(quality.get("warning_count")),
            }
    return {
        "status": "UNKNOWN",
        "source": "PRIOR_ARTIFACTS",
        "carried_forward_from_prior_artifacts": True,
        "error_count": None,
        "warning_count": None,
    }


def _requested_date_range(sources: Mapping[str, Any]) -> dict[str, Any]:
    for key in ("observation_protocol", "owner_gate", "sensitivity_result", "event_retest"):
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
    for key in ("observation_protocol", "owner_gate", "sensitivity_result", "event_retest"):
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
        "observation_mode": ready,
        "observation_dry_run_record": ready,
        "no_side_effect_evidence": ready,
        "observation_decision": ready,
        "owner_review_required": ready,
        "paper_shadow_enabled_false": ready,
        "paper_trade_created_false": ready,
        "shadow_position_created_false": ready,
        "event_append_enabled_false": ready,
        "event_append_attempted_false": ready,
        "outcome_binding_enabled_false": ready,
        "outcome_binding_attempted_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "daily_report_generated_false": ready,
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


def _number_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
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
