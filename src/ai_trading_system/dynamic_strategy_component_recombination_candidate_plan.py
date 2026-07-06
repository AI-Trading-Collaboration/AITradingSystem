from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision as m2391
import ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification as m2390
import ai_trading_system.dynamic_strategy_component_ablation_owner_review_decision as m2394
import ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan as m2392
import ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest as m2393
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2395"
TASK_REGISTER_ID = (
    "TRADING-2395_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN"
)
REPORT_TYPE = "dynamic_strategy_component_recombination_candidate_plan"
SCHEMA_VERSION = "dynamic_strategy_component_recombination_candidate_plan.v1"
READY_STATUS = "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = "TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2390",
    "TRADING-2391",
    "TRADING-2392",
    "TRADING-2393",
    "TRADING-2394",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA"
)

RETURN_ENGINE_COMPONENT = "growth_tilt_engine"
LOWER_TURNOVER_GUARDRAIL = "lower_turnover_guardrail"
GUARDED_TURNOVER_TRANSFER = "guarded_turnover_transfer"
VALID_UNTIL_WINDOW = "valid_until_window"
NO_STALE_SIGNAL_CARRY_FORWARD = "no_stale_signal_carry_forward"

PLANNED_RECOMBINATION_CANDIDATES: tuple[str, ...] = (
    "growth_tilt_lower_turnover_guarded_v1",
    "growth_tilt_turnover_budgeted_v1",
    "growth_tilt_valid_until_strict_v1",
    "growth_tilt_turnover_budgeted_valid_until_strict_v1",
    "growth_tilt_lower_turnover_guarded_transfer_v1",
    "growth_tilt_conservative_guarded_v1",
)
GUARDRAIL_COMPONENTS: tuple[str, ...] = (
    LOWER_TURNOVER_GUARDRAIL,
    VALID_UNTIL_WINDOW,
    NO_STALE_SIGNAL_CARRY_FORWARD,
    "turnover_budgeting",
    "cooldown_balancing",
    "max_single_step_weight_delta",
    "risk_cap_preservation",
)
OWNER_REVIEW_COMPONENTS: tuple[str, ...] = (GUARDED_TURNOVER_TRANSFER,)
REFERENCE_CANDIDATES: dict[str, dict[str, str]] = {
    "static_baseline": {"role": "baseline_reference"},
    "raw_growth_tilt_reference": {
        "candidate_id": RANKING_TOP_CANDIDATE,
        "role": "raw_return_engine_reference",
    },
    "lower_turnover_reference": {
        "candidate_id": "dynamic_regime_overlay_v0_4_lower_turnover",
        "role": "robustness_reference",
    },
    "cooldown_balanced_reference": {
        "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
        "role": "best_lower_turnover_variant_reference",
    },
    "guarded_turnover_reference": {
        "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
        "role": "guarded_transfer_reference",
    },
}
FORBIDDEN_RECOMBINATION_PATHS: tuple[str, ...] = (
    "raw_growth_tilt_without_guardrails",
    "use_monthly_rebalance_as_primary",
    "allow_stale_signal_carry_forward",
    "remove_valid_until_window",
    "remove_risk_cap_without_replacement",
    "remove_turnover_constraints_without_cost_stress",
    "optimize_only_for_total_return",
    "accept_candidate_without_static_baseline_comparison",
    "accept_candidate_without_raw_growth_tilt_reference_comparison",
    "accept_candidate_without_lower_turnover_reference_comparison",
    "accept_candidate_without_cost_stress",
    "approve_research_only_observation_in_plan_task",
    "enable_paper_shadow_or_scheduler",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "observation_approved",
    "current_best_candidate_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_shadow_allowed",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "event_append_approved",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "outcome_store_mutated",
    "production_enabled",
    "production_approved",
    "production_allowed",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "new_signal_generated",
    "scoring_run",
)

DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH = (
    m2390.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "reclassification_result.json"
)
DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH = (
    m2391.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH = (
    m2392.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "component_attribution_plan.json"
)
DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH = (
    m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "ablation_retest_result.json"
)
DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH = (
    m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "component_attribution_matrix.json"
)
DEFAULT_SOURCE_2393_REUSABLE_COMPONENT_DECISION_PATH = (
    m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "reusable_component_decision.json"
)
DEFAULT_SOURCE_2394_OWNER_REVIEW_DECISION_PATH = (
    m2394.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2394_COMPONENT_RECOMBINATION_DECISION_PATH = (
    m2394.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "component_recombination_decision.json"
)
DEFAULT_SOURCE_2394_RECOMBINATION_PRINCIPLES_PATH = (
    m2394.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "recombination_principles.json"
)


def run_dynamic_strategy_component_recombination_candidate_plan(
    *,
    source_reclassification_result_2390_path: Path = (
        DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH
    ),
    source_owner_review_decision_2391_path: Path = (
        DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH
    ),
    source_component_attribution_plan_2392_path: Path = (
        DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH
    ),
    source_ablation_retest_result_2393_path: Path = (
        DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH
    ),
    source_component_attribution_matrix_2393_path: Path = (
        DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH
    ),
    source_reusable_component_decision_2393_path: Path = (
        DEFAULT_SOURCE_2393_REUSABLE_COMPONENT_DECISION_PATH
    ),
    source_owner_review_decision_2394_path: Path = (
        DEFAULT_SOURCE_2394_OWNER_REVIEW_DECISION_PATH
    ),
    source_component_recombination_decision_2394_path: Path = (
        DEFAULT_SOURCE_2394_COMPONENT_RECOMBINATION_DECISION_PATH
    ),
    source_recombination_principles_2394_path: Path = (
        DEFAULT_SOURCE_2394_RECOMBINATION_PRINCIPLES_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_reclassification_result_2390_path=(
            source_reclassification_result_2390_path
        ),
        source_owner_review_decision_2391_path=source_owner_review_decision_2391_path,
        source_component_attribution_plan_2392_path=(
            source_component_attribution_plan_2392_path
        ),
        source_ablation_retest_result_2393_path=source_ablation_retest_result_2393_path,
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
        source_reusable_component_decision_2393_path=(
            source_reusable_component_decision_2393_path
        ),
        source_owner_review_decision_2394_path=source_owner_review_decision_2394_path,
        source_component_recombination_decision_2394_path=(
            source_component_recombination_decision_2394_path
        ),
        source_recombination_principles_2394_path=(
            source_recombination_principles_2394_path
        ),
    )
    ready = not sources["source_validation_errors"]
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=as_of_date,
        sources=sources,
    )
    payload.update(_ready_sections(sources) if ready else _blocked_sections(sources))
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(**paths: Path) -> dict[str, Any]:
    source_files = {
        key.removeprefix("source_").removesuffix("_path"): path
        for key, path in paths.items()
    }
    documents = {key: _load_json_document(path) for key, path in source_files.items()}
    source_status = {
        key: _as_mapping(document).get("status") for key, document in documents.items()
    }
    sources: dict[str, Any] = {
        **documents,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
        "source_status": source_status,
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    sources["source_ready_for_recombination_candidate_plan"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "reclassification_result_2390": m2390.READY_STATUS,
        "owner_review_decision_2391": m2391.READY_STATUS,
        "component_attribution_plan_2392": m2392.READY_STATUS,
        "ablation_retest_result_2393": m2393.READY_STATUS,
        "component_attribution_matrix_2393": m2393.READY_STATUS,
        "reusable_component_decision_2393": m2393.READY_STATUS,
        "owner_review_decision_2394": m2394.READY_STATUS,
        "component_recombination_decision_2394": m2394.READY_STATUS,
        "recombination_principles_2394": m2394.READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )
    _validate_2390_reclassification(sources, errors)
    _validate_2391_owner_decision(sources, errors)
    _validate_2392_component_plan(sources, errors)
    _validate_2393_component_ablation(sources, errors)
    _validate_2394_recombination_decision(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2390_reclassification(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    document = _as_mapping(sources.get("reclassification_result_2390"))
    if document.get("recommended_next_research_task") != m2390.NEXT_ROUTE:
        errors.append("2390 route did not point to TRADING-2391")
    if document.get("candidate_auto_accept_approved") is True:
        errors.append("2390 unexpectedly approved candidate auto-accept")
    if document.get("research_only_observation_approved") is True:
        errors.append("2390 unexpectedly approved research-only observation")
    if document.get("owner_review_recommendation_ready") is not True:
        errors.append("2390 owner review recommendation is not ready")


def _validate_2391_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    document = _as_mapping(sources.get("owner_review_decision_2391"))
    if document.get("owner_decision") != m2391.OWNER_DECISION:
        errors.append("2391 owner decision mismatch")
    if document.get("recommended_next_research_task") != m2391.NEXT_ROUTE:
        errors.append("2391 route did not point to TRADING-2392")
    if document.get("component_attribution_continue_recommended") is not True:
        errors.append("2391 did not continue component attribution")
    if document.get("research_only_observation_approved") is True:
        errors.append("2391 unexpectedly approved research-only observation")


def _validate_2392_component_plan(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    document = _as_mapping(sources.get("component_attribution_plan_2392"))
    if document.get("recommended_next_research_task") != m2392.NEXT_ROUTE:
        errors.append("2392 route did not point to TRADING-2393")
    if document.get("targeted_ablation_retest_plan_ready") is not True:
        errors.append("2392 targeted ablation retest plan is not ready")
    if set(m2392.COMPONENTS_TO_ATTRIBUTE) - set(
        document.get("components_to_attribute", [])
    ):
        errors.append("2392 missing components_to_attribute")
    if set(m2392.COMPONENT_VALUE_CANDIDATES) - set(
        document.get("component_value_candidates", [])
    ):
        errors.append("2392 missing component value candidates")


def _validate_2393_component_ablation(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("ablation_retest_result_2393"))
    matrix_doc = _as_mapping(sources.get("component_attribution_matrix_2393"))
    decision_doc = _as_mapping(sources.get("reusable_component_decision_2393"))
    decisions = _component_decisions_from_sources(sources)

    if retest.get("recommended_next_research_task") != m2393.NEXT_ROUTE:
        errors.append("2393 route did not point to TRADING-2394")
    if retest.get("best_reusable_component") != RETURN_ENGINE_COMPONENT:
        errors.append("2393 best reusable component is not growth_tilt_engine")
    if retest.get("ablation_retest_ready") is not True:
        errors.append("2393 ablation retest is not ready")
    if retest.get("data_quality_gate_executed") is not True:
        errors.append("2393 data quality gate was not executed")
    if retest.get("research_only_observation_approved") is True:
        errors.append("2393 unexpectedly approved research-only observation")
    if decisions.get(RETURN_ENGINE_COMPONENT) != m2393.COMPONENT_DECISION_REUSABLE:
        errors.append("2393 growth_tilt_engine decision mismatch")
    if decisions.get(LOWER_TURNOVER_GUARDRAIL) != m2393.COMPONENT_DECISION_GUARDRAIL:
        errors.append("2393 lower_turnover_guardrail decision mismatch")
    if decisions.get(GUARDED_TURNOVER_TRANSFER) != m2393.COMPONENT_DECISION_OWNER_REVIEW:
        errors.append("2393 guarded_turnover_transfer decision mismatch")

    matrix_rows = _as_list_of_mappings(matrix_doc.get("component_attribution_matrix"))
    matrix_components = {str(row.get("component_name")) for row in matrix_rows}
    if set(m2392.COMPONENTS_TO_ATTRIBUTE) - matrix_components:
        errors.append("2393 component attribution matrix missing components")
    reusable = _as_mapping(decision_doc.get("reusable_component_decision"))
    if reusable.get("best_reusable_component") != RETURN_ENGINE_COMPONENT:
        errors.append("2393 reusable decision best component mismatch")


def _validate_2394_recombination_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2394"))
    recombination_doc = _as_mapping(
        sources.get("component_recombination_decision_2394")
    )
    recombination = _as_mapping(
        recombination_doc.get("component_recombination_decision")
    )
    principles_doc = _as_mapping(sources.get("recombination_principles_2394"))
    principles = _as_mapping(principles_doc.get("recombination_principles"))

    if owner.get("owner_decision") != m2394.OWNER_DECISION:
        errors.append("2394 owner decision mismatch")
    if owner.get("recommended_next_research_task") != m2394.NEXT_ROUTE:
        errors.append("2394 route did not point to TRADING-2395")
    if owner.get("recombination_plan_approved") is not True:
        errors.append("2394 recombination plan was not approved")
    if owner.get("research_only_observation_approved") is True:
        errors.append("2394 unexpectedly approved research-only observation")
    if owner.get("growth_tilt_engine_adopted_as_return_engine") is not True:
        errors.append("2394 did not adopt growth_tilt_engine as return engine")
    if owner.get("lower_turnover_guardrail_adopted_as_guardrail_only") is not True:
        errors.append("2394 did not limit lower_turnover_guardrail to guardrail")
    if owner.get("guarded_turnover_transfer_requires_further_review") is not True:
        errors.append("2394 did not retain guarded transfer owner review")
    if recombination.get("record_ready") is not True:
        errors.append("2394 component recombination decision record is not ready")
    if recombination.get("owner_decision") != m2394.OWNER_DECISION:
        errors.append("2394 component recombination owner decision mismatch")
    if recombination.get("research_only_observation_approved") is True:
        errors.append("2394 recombination decision unexpectedly approved observation")
    if principles.get("return_engine", {}).get("primary") != RETURN_ENGINE_COMPONENT:
        errors.append("2394 recombination principles return engine mismatch")


def _validate_source_safety(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    for source_name in _source_document_names():
        document = _as_mapping(sources.get(source_name))
        if document.get("production_effect") not in (None, "none"):
            errors.append(f"{source_name}: production_effect must be none")
        if document.get("broker_action") not in (None, "none"):
            errors.append(f"{source_name}: broker_action must be none")
        for field in SAFETY_FALSE_FIELDS:
            if document.get(field) is True:
                errors.append(f"{source_name}: safety field must be false: {field}")


def _base_payload(
    *,
    status: str,
    as_of_date: date | None,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "as_of": (as_of_date or date.today()).isoformat(),
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "source_tasks": list(SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_recombination_candidate_plan": bool(
            sources.get("source_ready_for_recombination_candidate_plan")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": "NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY",
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "research_only": True,
        "observe_only": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in SAFETY_FALSE_FIELDS},
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    source_findings = _source_findings(sources)
    source_component_map = _source_component_map()
    principles = _recombination_principles()
    definitions = _recombination_candidate_definitions()
    retest_plan = _retest_plan_2396()
    acceptance_criteria = _acceptance_criteria()
    return {
        "owner_decision_from_2394": m2394.OWNER_DECISION,
        "recombination_candidate_plan_ready": True,
        "recombination_candidate_definitions_ready": True,
        "retest_plan_2396_ready": True,
        "acceptance_criteria_ready": True,
        "return_engine_component": RETURN_ENGINE_COMPONENT,
        "guardrail_components": list(GUARDRAIL_COMPONENTS),
        "owner_review_components": list(OWNER_REVIEW_COMPONENTS),
        "source_component_map": source_component_map,
        "source_findings": source_findings,
        "recombination_principles": principles,
        "planned_recombination_candidates": list(PLANNED_RECOMBINATION_CANDIDATES),
        "recombination_candidate_definitions": definitions,
        "reference_candidates": REFERENCE_CANDIDATES,
        "forbidden_recombination_paths": list(FORBIDDEN_RECOMBINATION_PATHS),
        "retest_plan_2396": retest_plan,
        "recombination_acceptance_criteria": acceptance_criteria,
        "explicit_non_approval_list": _explicit_non_approval_list(),
        "guardrail_summary": _guardrail_summary(),
        "next_route": _next_route_record(),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "owner_decision_from_2394": None,
        "recombination_candidate_plan_ready": False,
        "recombination_candidate_definitions_ready": False,
        "retest_plan_2396_ready": False,
        "acceptance_criteria_ready": False,
        "return_engine_component": None,
        "guardrail_components": [],
        "owner_review_components": [],
        "source_component_map": _source_component_map(),
        "source_findings": _source_findings(sources),
        "recombination_principles": _recombination_principles(),
        "planned_recombination_candidates": [],
        "recombination_candidate_definitions": [],
        "reference_candidates": REFERENCE_CANDIDATES,
        "forbidden_recombination_paths": list(FORBIDDEN_RECOMBINATION_PATHS),
        "retest_plan_2396": _retest_plan_2396(),
        "recombination_acceptance_criteria": _acceptance_criteria(),
        "explicit_non_approval_list": _explicit_non_approval_list(),
        "guardrail_summary": _guardrail_summary(),
        "next_route": {"record_ready": False},
        "recommended_next_research_task": None,
        "blocked_until": list(sources.get("source_validation_errors", [])),
    }


def _source_component_map() -> dict[str, dict[str, Any]]:
    return {
        RETURN_ENGINE_COMPONENT: {
            "source_candidate": RANKING_TOP_CANDIDATE,
            "source_tasks": ["TRADING-2365", "TRADING-2386", "TRADING-2393", "TRADING-2394"],
            "adopted_role": "RETURN_ENGINE",
            "status": "ADOPTED_AS_REUSABLE_RETURN_ENGINE",
        },
        LOWER_TURNOVER_GUARDRAIL: {
            "source_candidates": [
                "dynamic_regime_overlay_v0_4_lower_turnover",
                "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
            ],
            "source_tasks": ["TRADING-2379", "TRADING-2393", "TRADING-2394"],
            "adopted_role": "EXECUTION_AND_RISK_GUARDRAIL",
            "status": "USE_ONLY_AS_GUARDRAIL",
        },
        GUARDED_TURNOVER_TRANSFER: {
            "source_candidate": "equal_risk_growth_tilt_guarded_turnover_v1",
            "source_tasks": ["TRADING-2383", "TRADING-2393", "TRADING-2394"],
            "adopted_role": "OWNER_REVIEW_COMPONENT",
            "status": "OWNER_REVIEW_REQUIRED",
        },
        VALID_UNTIL_WINDOW: {
            "source_tasks": ["TRADING-2364", "TRADING-2357", "TRADING-2388"],
            "adopted_role": "HARD_RESEARCH_EXECUTION_GUARDRAIL",
            "status": "REQUIRED",
        },
        NO_STALE_SIGNAL_CARRY_FORWARD: {
            "source_tasks": ["TRADING-2357", "TRADING-2388", "TRADING-2392"],
            "adopted_role": "HARD_RESEARCH_GUARDRAIL",
            "status": "REQUIRED",
        },
    }


def _recombination_principles() -> dict[str, Any]:
    return {
        "primary_return_engine": {
            "component": RETURN_ENGINE_COMPONENT,
            "purpose": "preserve upside capture and return advantage",
        },
        "guardrail_layer": {
            "components": [
                LOWER_TURNOVER_GUARDRAIL,
                VALID_UNTIL_WINDOW,
                NO_STALE_SIGNAL_CARRY_FORWARD,
                "cooldown_balancing",
                "max_single_step_weight_delta",
                "turnover_budgeting_if_supported",
                "risk_cap_preservation",
            ],
        },
        "owner_review_layer": {"components": [GUARDED_TURNOVER_TRANSFER]},
        "primary_execution_cadence": VALID_UNTIL_WINDOW,
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "must_preserve": [
            VALID_UNTIL_WINDOW,
            NO_STALE_SIGNAL_CARRY_FORWARD,
            "cost_stress_testing",
            "turnover_budget",
            "paper_shadow_disabled",
            "scheduler_disabled",
            "broker_disabled",
        ],
        "must_not": [
            "optimize_only_for_total_return",
            "remove_risk_guardrails",
            "allow_stale_signal_carry_forward",
            "rely_on_monthly_rebalance",
            "approve_observation_without_recombined_retest",
        ],
    }


def _recombination_candidate_definitions() -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": "growth_tilt_lower_turnover_guarded_v1",
            "purpose": (
                "combine primary return engine with lower-turnover execution "
                "guardrail"
            ),
            "components": [
                RETURN_ENGINE_COMPONENT,
                LOWER_TURNOVER_GUARDRAIL,
                VALID_UNTIL_WINDOW,
                NO_STALE_SIGNAL_CARRY_FORWARD,
                "max_single_step_weight_delta",
            ],
            "hypothesis": [
                "preserve meaningful upside from growth_tilt_engine",
                "reduce turnover and cost drag using lower_turnover_guardrail",
                "avoid stale signal execution",
            ],
            "expected_tradeoff": [
                "return may decline vs raw ranking top",
                "turnover / drawdown should improve",
            ],
            "owner_review_required": False,
        },
        {
            "candidate_id": "growth_tilt_turnover_budgeted_v1",
            "purpose": (
                "test whether explicit turnover budget can preserve return "
                "while reducing cost drag"
            ),
            "components": [RETURN_ENGINE_COMPONENT, "turnover_budgeting", VALID_UNTIL_WINDOW],
            "hypothesis": [
                "turnover budget can reduce unnecessary rebalances",
                "cost-adjusted return improves relative to raw growth tilt",
            ],
            "expected_tradeoff": [
                "some upside may be lost",
                "turnover budget should improve robustness",
            ],
            "owner_review_required": False,
        },
        {
            "candidate_id": "growth_tilt_valid_until_strict_v1",
            "purpose": (
                "test whether stricter signal expiry improves stale-signal "
                "discipline"
            ),
            "components": [
                RETURN_ENGINE_COMPONENT,
                "valid_until_strictness",
                NO_STALE_SIGNAL_CARRY_FORWARD,
            ],
            "hypothesis": [
                "stale signal execution decreases",
                "near-expiry overreaction decreases",
                "upside capture remains acceptable",
            ],
            "expected_tradeoff": [
                "stricter expiry may reduce return",
                "signal discipline should improve",
            ],
            "owner_review_required": False,
        },
        {
            "candidate_id": "growth_tilt_turnover_budgeted_valid_until_strict_v1",
            "purpose": (
                "combine the two most relevant execution guardrails with "
                "growth tilt"
            ),
            "components": [
                RETURN_ENGINE_COMPONENT,
                "turnover_budgeting",
                "valid_until_strictness",
                NO_STALE_SIGNAL_CARRY_FORWARD,
            ],
            "hypothesis": [
                "combined execution guardrails improve cost-adjusted robustness",
                "return remains positive vs static",
                "stale signal and turnover both improve",
            ],
            "expected_tradeoff": [
                "possible upside reduction",
                "improved observation-gate evidence if tradeoff is acceptable",
            ],
            "owner_review_required": False,
        },
        {
            "candidate_id": "growth_tilt_lower_turnover_guarded_transfer_v1",
            "purpose": "test guarded_turnover_transfer as owner-review component",
            "components": [
                RETURN_ENGINE_COMPONENT,
                LOWER_TURNOVER_GUARDRAIL,
                GUARDED_TURNOVER_TRANSFER,
                VALID_UNTIL_WINDOW,
            ],
            "hypothesis": [
                "guarded transfer may preserve more ranking-top upside than "
                "lower-turnover guardrail alone",
                "turnover and drawdown should improve vs raw ranking top",
            ],
            "expected_tradeoff": [
                "owner review required due to transfer uncertainty",
            ],
            "owner_review_required": True,
        },
        {
            "candidate_id": "growth_tilt_conservative_guarded_v1",
            "purpose": "conservative recombination for robustness stress",
            "components": [
                RETURN_ENGINE_COMPONENT,
                LOWER_TURNOVER_GUARDRAIL,
                "strict_risk_cap",
                "cooldown_balancing",
                VALID_UNTIL_WINDOW,
                NO_STALE_SIGNAL_CARRY_FORWARD,
            ],
            "hypothesis": [
                "robust under conservative / harsh cost",
                "drawdown improves",
                "return gap may remain",
            ],
            "expected_tradeoff": [
                "lower upside capture",
                "potentially better gate stability",
            ],
            "owner_review_required": False,
        },
    ]


def _retest_plan_2396() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_component_recombination_retest_plan.v1",
        "plan_ready": True,
        "next_task": NEXT_ROUTE,
        "execution_cadence": {
            "primary": VALID_UNTIL_WINDOW,
            "comparison": [
                VALID_UNTIL_WINDOW,
                "cooldown_limited_event_driven",
                "signal_event_driven",
            ],
            "monthly_rebalance": {
                "allowed_for_reference": True,
                "allowed_for_primary_decision": False,
            },
        },
        "cost_stress": {
            "base": {"transaction_cost_bps": 2, "slippage_bps": 2},
            "realistic": {"transaction_cost_bps": 5, "slippage_bps": 5},
            "conservative": {"transaction_cost_bps": 10, "slippage_bps": 10},
            "harsh": {"transaction_cost_bps": 20, "slippage_bps": 10},
        },
        "slice_regime_tests": {
            "time_slices": [
                "full_available_window",
                "recent_period",
                "post_2023_ai_cycle",
                "high_volatility_periods",
                "drawdown_recovery_periods",
            ],
            "regime_slices": [
                "risk_on",
                "risk_off",
                "high_volatility",
                "low_volatility",
                "trend_confirmed",
                "recovery",
            ],
        },
        "component_evidence_metrics": {
            "return_engine": [
                "return_retention_vs_raw_growth_tilt",
                "upside_capture",
                "dynamic_vs_static_gap",
            ],
            "guardrail_layer": [
                "turnover_reduction_vs_raw_growth_tilt",
                "cost_drag_reduction",
                "drawdown_gap_vs_static",
                "max_monthly_turnover",
            ],
            "valid_until_layer": [
                "stale_signal_execution_count",
                "signal_to_execution_lag_days",
                "near_expiry_signal_behavior",
            ],
            "recombination_quality": [
                "cost_adjusted_return",
                "return_per_drawdown_penalty",
                "time_slice_pass_rate",
                "regime_expectation_score",
                "owner_review_required",
            ],
        },
        "planned_recombination_candidates": list(PLANNED_RECOMBINATION_CANDIDATES),
        "reference_candidates": REFERENCE_CANDIDATES,
        "must_not": [
            "run_without_data_quality_gate_in_2396",
            "use_monthly_rebalance_as_primary",
            "approve_observation_inside_retest",
            "enable_scheduler_or_paper_shadow",
            "call_broker_or_generate_order",
        ],
    }


def _acceptance_criteria() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_recombination_acceptance_criteria.v1",
        "owner_review_candidate_criteria": {
            "must": [
                "cost_adjusted_return_above_static",
                "survives_realistic_cost",
                "survives_conservative_cost",
                "valid_until_window_preserved",
                "no_stale_signal_carry_forward",
                "turnover_not_materially_worse_than_raw_growth_tilt",
                "drawdown_tradeoff_explainable",
            ],
            "should": [
                "preserve_meaningful_growth_tilt_return",
                "reduce_turnover_vs_raw_growth_tilt",
                "improve_drawdown_vs_raw_growth_tilt",
                "improve_or_preserve_time_slice_evidence",
                "improve_or_preserve_regime_evidence",
            ],
            "must_not": [
                "rely_on_monthly_rebalance",
                "require_scheduler",
                "require_event_append",
                "require_outcome_binding",
                "require_paper_shadow",
                "require_production_or_broker",
            ],
        },
        "observation_preview_criteria": {
            "must": [
                "owner_review_candidate_criteria_passed",
                "time_slice_evidence_not_weak",
                "regime_expectation_score_not_weak",
                "drawdown_materiality_not_severe",
                "no_major_guardrail_failure",
            ],
            "note": "actual observation approval must remain separate owner decision",
        },
    }


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    reclassification = _as_mapping(sources.get("reclassification_result_2390"))
    owner_2391 = _as_mapping(sources.get("owner_review_decision_2391"))
    plan_2392 = _as_mapping(sources.get("component_attribution_plan_2392"))
    retest_2393 = _as_mapping(sources.get("ablation_retest_result_2393"))
    owner_2394 = _as_mapping(sources.get("owner_review_decision_2394"))
    return {
        "trading_2390": {
            "status": reclassification.get("status"),
            "owner_review_recommendation_ready": reclassification.get(
                "owner_review_recommendation_ready"
            ),
            "recommended_next_research_task": reclassification.get(
                "recommended_next_research_task"
            ),
        },
        "trading_2391": {
            "status": owner_2391.get("status"),
            "owner_decision": owner_2391.get("owner_decision"),
            "component_attribution_continue_recommended": owner_2391.get(
                "component_attribution_continue_recommended"
            ),
            "research_only_observation_approved": owner_2391.get(
                "research_only_observation_approved"
            ),
        },
        "trading_2392": {
            "status": plan_2392.get("status"),
            "component_value_candidates": plan_2392.get("component_value_candidates"),
            "components_to_attribute": plan_2392.get("components_to_attribute"),
            "targeted_ablation_retest_plan_ready": plan_2392.get(
                "targeted_ablation_retest_plan_ready"
            ),
        },
        "trading_2393": {
            "status": retest_2393.get("status"),
            "best_reusable_component": retest_2393.get("best_reusable_component"),
            "component_decisions": _component_decisions_from_sources(sources),
            "data_quality_status": retest_2393.get("data_quality_status"),
        },
        "trading_2394": {
            "status": owner_2394.get("status"),
            "owner_decision": owner_2394.get("owner_decision"),
            "recombination_plan_approved": owner_2394.get(
                "recombination_plan_approved"
            ),
            "recommended_next_research_task": owner_2394.get(
                "recommended_next_research_task"
            ),
        },
    }


def _explicit_non_approval_list() -> list[str]:
    return [
        "candidate_auto_accept",
        "research_only_observation",
        "paper_shadow",
        "paper_trade",
        "shadow_position",
        "event_append",
        "outcome_binding",
        "scheduler",
        "scheduled_task",
        "daily_report",
        "production",
        "broker_order",
        "new_backtest",
        "new_signal",
        "scoring",
    ]


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "RECOMBINATION_CANDIDATE_PLAN_ONLY",
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _next_route_record() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": (
            "recombination candidates are planned but require TRADING-2396 "
            "actual retest before any observation owner decision"
        ),
        "candidate_plan_ready": True,
        "retest_required_before_owner_observation_decision": True,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "json_path": str(output_root / "recombination_candidate_plan.json"),
        "candidate_definitions_json": str(
            output_root / "recombination_candidate_definitions.json"
        ),
        "retest_plan_2396_json": str(output_root / "retest_plan_2396.json"),
        "acceptance_criteria_json": str(
            output_root / "recombination_acceptance_criteria.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_component_recombination_candidate_plan.md"
        ),
        "candidate_definitions_markdown": str(
            docs_root / "dynamic_strategy_recombination_candidate_definitions.md"
        ),
        "retest_plan_2396_markdown": str(
            docs_root / "dynamic_strategy_recombination_retest_plan.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2396_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["candidate_definitions_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_candidate_definitions",
            "schema_version": "dynamic_strategy_recombination_candidate_definitions.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "recombination_candidate_definitions": payload.get(
                "recombination_candidate_definitions"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["retest_plan_2396_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_retest_plan_2396",
            "schema_version": "dynamic_strategy_recombination_retest_plan_2396.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "retest_plan_2396": payload.get("retest_plan_2396"),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["acceptance_criteria_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_acceptance_criteria",
            "schema_version": "dynamic_strategy_recombination_acceptance_criteria.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "recombination_acceptance_criteria": payload.get(
                "recombination_acceptance_criteria"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["candidate_definitions_markdown"]),
        _candidate_definitions_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["retest_plan_2396_markdown"]),
        _retest_plan_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 component recombination candidate plan",
            "",
            f"- status：`{payload.get('status')}`",
            f"- return engine：`{payload.get('return_engine_component')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Executive summary",
            "",
            "TRADING-2395 只设计 recombination candidates：以 "
            "`growth_tilt_engine` 作为收益引擎，把 "
            "`lower_turnover_guardrail` 放在 execution / risk guardrail 层，"
            "并保留 `guarded_turnover_transfer` 为 owner-review component。"
            "本任务不运行 backtest、不生成 signal、不批准 observation、paper-shadow、"
            "scheduler、event append、outcome binding、production 或 broker。",
            "",
            "## Source findings from TRADING-2393 / 2394",
            "",
            "```json",
            _json_block(payload.get("source_findings")),
            "```",
            "",
            "## Recombination principles",
            "",
            "```json",
            _json_block(payload.get("recombination_principles")),
            "```",
            "",
            "## Source component map",
            "",
            "```json",
            _json_block(payload.get("source_component_map")),
            "```",
            "",
            "## Recombination candidate definitions",
            "",
            "```json",
            _json_block(payload.get("recombination_candidate_definitions")),
            "```",
            "",
            "## Forbidden recombination paths",
            "",
            "```json",
            _json_block(payload.get("forbidden_recombination_paths")),
            "```",
            "",
            "## TRADING-2396 retest plan",
            "",
            "```json",
            _json_block(payload.get("retest_plan_2396")),
            "```",
            "",
            "## Acceptance criteria",
            "",
            "```json",
            _json_block(payload.get("recombination_acceptance_criteria")),
            "```",
            "",
            "## Explicit non-approval list",
            "",
            "```json",
            _json_block(payload.get("explicit_non_approval_list")),
            "```",
            "",
            "## Guardrail summary",
            "",
            "```json",
            _json_block(payload.get("guardrail_summary")),
            "```",
            "",
            "## Recommended next route",
            "",
            f"`{payload.get('recommended_next_research_task')}`",
        ]
    )


def _candidate_definitions_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 recombination candidate definitions",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            "```json",
            _json_block(payload.get("recombination_candidate_definitions")),
            "```",
        ]
    )


def _retest_plan_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 recombination retest plan",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("retest_plan_2396")),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 TRADING-2396 路由",
            "",
            f"- status：`{payload.get('status')}`",
            f"- 推荐下一路由：`{payload.get('recommended_next_research_task')}`",
            "- 下一步：component recombination candidate retest",
            "- observation approved：`False`",
            "- paper-shadow enabled：`False`",
            "- scheduler enabled：`False`",
            "- production enabled：`False`",
            "- broker action enabled：`False`",
            "",
            "TRADING-2396 才能执行 recombination candidate retest；2395 只提供 "
            "candidate plan 和 retest plan。",
        ]
    )


def _component_decisions_from_sources(sources: Mapping[str, Any]) -> dict[str, str]:
    retest = _as_mapping(sources.get("ablation_retest_result_2393"))
    decisions = {
        str(key): str(value)
        for key, value in _as_mapping(retest.get("component_decisions")).items()
    }
    if decisions:
        return decisions
    decision_doc = _as_mapping(sources.get("reusable_component_decision_2393"))
    reusable = _as_mapping(decision_doc.get("reusable_component_decision"))
    return {
        str(key): str(value)
        for key, value in _as_mapping(reusable.get("component_decisions")).items()
    }


def _load_json_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "missing_path": str(path)}
    return json.loads(path.read_text(encoding="utf-8"))


def _source_document_names() -> tuple[str, ...]:
    return (
        "reclassification_result_2390",
        "owner_review_decision_2391",
        "component_attribution_plan_2392",
        "ablation_retest_result_2393",
        "component_attribution_matrix_2393",
        "reusable_component_decision_2393",
        "owner_review_decision_2394",
        "component_recombination_decision_2394",
        "recombination_principles_2394",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_block(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
