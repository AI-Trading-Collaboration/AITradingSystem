from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision import (
    NEXT_ROUTE as SOURCE_2391_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision import (
    OWNER_DECISION as SOURCE_2391_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision import (
    READY_STATUS as SOURCE_2391_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan import (
    COMPONENT_VALUE_CANDIDATES,
    COMPONENTS_TO_ATTRIBUTE,
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan import (
    NEXT_ROUTE as SOURCE_2392_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan import (
    READY_STATUS as SOURCE_2392_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest import (
    COMPONENT_DECISION_GUARDRAIL,
    COMPONENT_DECISION_OWNER_REVIEW,
    COMPONENT_DECISION_REUSABLE,
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest import (
    NEXT_ROUTE as SOURCE_2393_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest import (
    READY_STATUS as SOURCE_2393_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2394"
TASK_REGISTER_ID = (
    "TRADING-2394_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_AND_"
    "RECOMBINATION_DECISION"
)
REPORT_TYPE = "dynamic_strategy_component_ablation_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_component_ablation_owner_review_and_recombination_decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_AND_RECOMBINATION_"
    "DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_AND_RECOMBINATION_"
    "DECISION_BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = "APPROVE_COMPONENT_RECOMBINATION_PLAN_WITH_NO_OBSERVATION_APPROVAL"
NEXT_ROUTE = "TRADING-2395_Dynamic_Strategy_Component_Recombination_Candidate_Plan"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REVIEW_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2391", "TRADING-2392", "TRADING-2393")
BEST_REUSABLE_COMPONENT = "growth_tilt_engine"
LOWER_TURNOVER_GUARDRAIL = "lower_turnover_guardrail"
GUARDED_TURNOVER_TRANSFER = "guarded_turnover_transfer"
ADOPT_GROWTH_TILT_DECISION = "APPROVE_AS_REUSABLE_RETURN_ENGINE"
ADOPT_LOWER_TURNOVER_DECISION = "APPROVE_AS_GUARDRAIL_ONLY"
GUARDED_TRANSFER_DECISION = "KEEP_OWNER_REVIEW_REQUIRED"
REJECT_DECISION = "REJECT"
APPROVE_DECISION = "APPROVE"

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
)

DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "component_attribution_plan.json"
)
DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "ablation_retest_result.json"
)
DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "component_attribution_matrix.json"
)
DEFAULT_SOURCE_2393_REUSABLE_COMPONENT_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "reusable_component_decision.json"
)
DEFAULT_SOURCE_2393_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_component_ablation_owner_review_decision(
    *,
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
    source_decision_update_2393_path: Path = DEFAULT_SOURCE_2393_DECISION_UPDATE_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
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
        source_decision_update_2393_path=source_decision_update_2393_path,
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
    sources["source_ready_for_owner_review_decision"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "owner_review_decision_2391": SOURCE_2391_READY_STATUS,
        "component_attribution_plan_2392": SOURCE_2392_READY_STATUS,
        "ablation_retest_result_2393": SOURCE_2393_READY_STATUS,
        "component_attribution_matrix_2393": SOURCE_2393_READY_STATUS,
        "reusable_component_decision_2393": SOURCE_2393_READY_STATUS,
        "decision_update_2393": SOURCE_2393_READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )

    _validate_2391_owner_decision(sources, errors)
    _validate_2392_component_plan(sources, errors)
    _validate_2393_component_ablation(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2391_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2391"))
    if owner.get("owner_decision") != SOURCE_2391_OWNER_DECISION:
        errors.append("2391 owner decision mismatch")
    if owner.get("recommended_next_research_task") != SOURCE_2391_EXPECTED_ROUTE:
        errors.append("2391 route did not point to TRADING-2392")
    if owner.get("research_only_observation_approved") is True:
        errors.append("2391 unexpectedly approved research-only observation")
    if owner.get("component_attribution_continue_recommended") is not True:
        errors.append("2391 did not recommend continuing component attribution")


def _validate_2392_component_plan(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    plan = _as_mapping(sources.get("component_attribution_plan_2392"))
    if plan.get("recommended_next_research_task") != SOURCE_2392_EXPECTED_ROUTE:
        errors.append("2392 route did not point to TRADING-2393")
    if plan.get("targeted_ablation_retest_plan_ready") is not True:
        errors.append("2392 targeted ablation retest plan is not ready")
    if set(COMPONENTS_TO_ATTRIBUTE) - set(plan.get("components_to_attribute", [])):
        errors.append("2392 missing required components_to_attribute")
    if set(COMPONENT_VALUE_CANDIDATES) - set(plan.get("component_value_candidates", [])):
        errors.append("2392 missing required component value candidates")


def _validate_2393_component_ablation(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("ablation_retest_result_2393"))
    matrix_doc = _as_mapping(sources.get("component_attribution_matrix_2393"))
    decision_doc = _as_mapping(sources.get("reusable_component_decision_2393"))
    update_doc = _as_mapping(sources.get("decision_update_2393"))
    decisions = _component_decisions_from_sources(sources)

    if retest.get("recommended_next_research_task") != SOURCE_2393_EXPECTED_ROUTE:
        errors.append("2393 route did not point to TRADING-2394")
    if retest.get("best_reusable_component") != BEST_REUSABLE_COMPONENT:
        errors.append("2393 best reusable component is not growth_tilt_engine")
    if retest.get("ablation_retest_ready") is not True:
        errors.append("2393 ablation retest is not ready")
    if retest.get("component_attribution_matrix_ready") is not True:
        errors.append("2393 component attribution matrix is not ready")
    if retest.get("reusable_component_decision_ready") is not True:
        errors.append("2393 reusable component decision is not ready")
    if retest.get("data_quality_gate_executed") is not True:
        errors.append("2393 data quality gate was not executed")
    if retest.get("research_only_observation_approved") is True:
        errors.append("2393 unexpectedly approved research-only observation")
    if decisions.get(BEST_REUSABLE_COMPONENT) != COMPONENT_DECISION_REUSABLE:
        errors.append("2393 growth_tilt_engine decision mismatch")
    if decisions.get(LOWER_TURNOVER_GUARDRAIL) != COMPONENT_DECISION_GUARDRAIL:
        errors.append("2393 lower_turnover_guardrail decision mismatch")
    if decisions.get(GUARDED_TURNOVER_TRANSFER) != COMPONENT_DECISION_OWNER_REVIEW:
        errors.append("2393 guarded_turnover_transfer decision mismatch")

    matrix_rows = _as_list_of_mappings(matrix_doc.get("component_attribution_matrix"))
    matrix_components = {str(row.get("component_name")) for row in matrix_rows}
    if set(COMPONENTS_TO_ATTRIBUTE) - matrix_components:
        errors.append("2393 component attribution matrix missing required components")
    reusable_decision = _as_mapping(decision_doc.get("reusable_component_decision"))
    if reusable_decision.get("best_reusable_component") != BEST_REUSABLE_COMPONENT:
        errors.append("2393 reusable component decision best component mismatch")
    decision_update = _as_mapping(update_doc.get("decision_update"))
    if decision_update.get("recommended_next_research_task") != SOURCE_2393_EXPECTED_ROUTE:
        errors.append("2393 decision update route mismatch")


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
        "source_ready_for_owner_review_decision": bool(
            sources.get("source_ready_for_owner_review_decision")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": "NOT_APPLICABLE_PRIOR_ARTIFACT_ONLY",
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "observe_only": False,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in SAFETY_FALSE_FIELDS},
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    decisions = _component_decisions_from_sources(sources)
    decision_inputs = _decision_inputs(sources, decisions)
    owner_items = _owner_decision_items()
    recombination_principles = _recombination_principles()
    recombination_decision = _component_recombination_decision(
        decision_inputs=decision_inputs,
        recombination_principles=recombination_principles,
    )
    next_route = _next_route_record()
    return {
        "owner_review_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "best_reusable_component": BEST_REUSABLE_COMPONENT,
        "growth_tilt_engine_adopted_as_return_engine": True,
        "growth_tilt_engine_decision": COMPONENT_DECISION_REUSABLE,
        "lower_turnover_guardrail_decision": COMPONENT_DECISION_GUARDRAIL,
        "lower_turnover_guardrail_adopted_as_guardrail_only": True,
        "guarded_turnover_transfer_decision": COMPONENT_DECISION_OWNER_REVIEW,
        "guarded_turnover_transfer_requires_further_review": True,
        "recombination_plan_approved": True,
        "component_value_candidates": list(COMPONENT_VALUE_CANDIDATES),
        "component_decisions": decisions,
        "decision_inputs": decision_inputs,
        "owner_decision_items": owner_items,
        "component_recombination_decision": recombination_decision,
        "recombination_principles": recombination_principles,
        "observation_non_approval_record": _observation_non_approval_record(),
        "explicit_non_approval_list": _explicit_non_approval_list(),
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources, decisions),
        "next_route": next_route,
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "owner_review_decision_recorded": False,
        "owner_decision": None,
        "best_reusable_component": None,
        "growth_tilt_engine_adopted_as_return_engine": False,
        "growth_tilt_engine_decision": None,
        "lower_turnover_guardrail_decision": None,
        "lower_turnover_guardrail_adopted_as_guardrail_only": False,
        "guarded_turnover_transfer_decision": None,
        "guarded_turnover_transfer_requires_further_review": False,
        "recombination_plan_approved": False,
        "component_value_candidates": [],
        "component_decisions": {},
        "decision_inputs": {},
        "owner_decision_items": _owner_decision_items(),
        "component_recombination_decision": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "recombination_principles": _recombination_principles(),
        "observation_non_approval_record": _observation_non_approval_record(),
        "explicit_non_approval_list": _explicit_non_approval_list(),
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources, {}),
        "next_route": {},
        "recommended_next_research_task": None,
    }


def _decision_inputs(
    sources: Mapping[str, Any],
    decisions: Mapping[str, str],
) -> dict[str, Any]:
    plan = _as_mapping(sources.get("component_attribution_plan_2392"))
    retest = _as_mapping(sources.get("ablation_retest_result_2393"))
    return {
        "best_reusable_component": {
            "component": BEST_REUSABLE_COMPONENT,
            "decision": decisions.get(BEST_REUSABLE_COMPONENT),
            "source_task": "TRADING-2393",
        },
        "lower_turnover_guardrail": {
            "component": LOWER_TURNOVER_GUARDRAIL,
            "decision": decisions.get(LOWER_TURNOVER_GUARDRAIL),
            "source_task": "TRADING-2393",
        },
        "guarded_turnover_transfer": {
            "component": GUARDED_TURNOVER_TRANSFER,
            "decision": decisions.get(GUARDED_TURNOVER_TRANSFER),
            "source_task": "TRADING-2393",
        },
        "component_value_candidates": list(plan.get("component_value_candidates", [])),
        "component_attribution_components": list(plan.get("components_to_attribute", [])),
        "recombination_candidate_direction": _as_mapping(
            retest.get("reusable_component_decision")
        ).get("recombination_candidate_direction"),
        "source_tasks": list(SOURCE_TASKS),
        "ranking_top_source_candidate": RANKING_TOP_CANDIDATE,
    }


def _owner_decision_items() -> dict[str, dict[str, Any]]:
    return {
        "adopt_growth_tilt_engine": {
            "recommended_decision": ADOPT_GROWTH_TILT_DECISION,
            "approved": True,
            "reason": "best reusable component from TRADING-2393",
        },
        "adopt_lower_turnover_guardrail": {
            "recommended_decision": ADOPT_LOWER_TURNOVER_DECISION,
            "approved": True,
            "reason": (
                "useful for cost / turnover discipline but not sufficient as "
                "return engine"
            ),
        },
        "guarded_turnover_transfer": {
            "recommended_decision": GUARDED_TRANSFER_DECISION,
            "approved": True,
            "reason": "potential value but tradeoff remains uncertain",
        },
        "approve_observation": {
            "recommended_decision": REJECT_DECISION,
            "approved": False,
            "reason": "no recombined candidate has been tested yet",
        },
        "approve_paper_shadow": {
            "recommended_decision": REJECT_DECISION,
            "approved": False,
            "reason": "out of scope and no execution approval",
        },
        "proceed_to_recombination_plan": {
            "recommended_decision": APPROVE_DECISION,
            "approved": True,
            "next_task": NEXT_ROUTE,
        },
    }


def _component_recombination_decision(
    *,
    decision_inputs: Mapping[str, Any],
    recombination_principles: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_component_recombination_decision.v1",
        "owner_decision": OWNER_DECISION,
        "recombination_plan_approved": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "decision_inputs": dict(decision_inputs),
        "recombination_principles": dict(recombination_principles),
        "design_direction": (
            "RECOMBINE_GROWTH_TILT_RETURN_ENGINE_WITH_LOWER_TURNOVER_GUARDRAILS"
        ),
        "must_not_start_retest_in_2394": True,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }


def _recombination_principles() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_recombination_principles.v1",
        "return_engine": {
            "primary": BEST_REUSABLE_COMPONENT,
            "source": RANKING_TOP_CANDIDATE,
        },
        "guardrail_layer": {
            "primary": [
                LOWER_TURNOVER_GUARDRAIL,
                "valid_until_window",
                "no_stale_signal_carry_forward",
                "turnover_budgeting_if_supported",
                "cooldown_balancing_if_supported",
            ],
        },
        "owner_review_layer": [GUARDED_TURNOVER_TRANSFER],
        "must_preserve": [
            "valid_until_window",
            "cost_stress_testing",
            "turnover_budget",
            "no_paper_shadow",
            "no_scheduler",
            "no_broker",
        ],
        "must_not": [
            "use_monthly_rebalance_as_primary",
            "optimize_only_for_total_return",
            "remove_risk_guardrails",
            "approve_observation_without_recombined_retest",
        ],
        "candidate_plan_boundary": (
            "TRADING-2395 may design recombination candidates; TRADING-2394 "
            "does not run retest or approve observation"
        ),
    }


def _observation_non_approval_record() -> dict[str, Any]:
    return {
        "record_ready": True,
        "owner_decision": OWNER_DECISION,
        "research_only_observation_approved": False,
        "candidate_auto_accept_approved": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "non_approval_reasons": [
            "no recombined candidate exists yet",
            "no recombined candidate has passed a targeted retest",
            "paper-shadow and execution gates remain separate and closed",
            "2394 is an owner decision record, not an execution task",
        ],
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
    ]


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "OWNER_REVIEW_AND_RECOMBINATION_DECISION_ONLY",
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
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


def _source_findings(
    sources: Mapping[str, Any],
    decisions: Mapping[str, str],
) -> dict[str, Any]:
    owner = _as_mapping(sources.get("owner_review_decision_2391"))
    plan = _as_mapping(sources.get("component_attribution_plan_2392"))
    retest = _as_mapping(sources.get("ablation_retest_result_2393"))
    return {
        "trading_2391": {
            "status": owner.get("status"),
            "owner_decision": owner.get("owner_decision"),
            "research_only_observation_approved": owner.get(
                "research_only_observation_approved"
            ),
            "recommended_next_research_task": owner.get(
                "recommended_next_research_task"
            ),
        },
        "trading_2392": {
            "status": plan.get("status"),
            "component_value_candidates": plan.get("component_value_candidates"),
            "components_to_attribute": plan.get("components_to_attribute"),
            "targeted_ablation_retest_plan_ready": plan.get(
                "targeted_ablation_retest_plan_ready"
            ),
            "recommended_next_research_task": plan.get(
                "recommended_next_research_task"
            ),
        },
        "trading_2393": {
            "status": retest.get("status"),
            "best_reusable_component": retest.get("best_reusable_component"),
            "component_decisions": dict(decisions),
            "data_quality_status": retest.get("data_quality_status"),
            "recommended_next_research_task": retest.get(
                "recommended_next_research_task"
            ),
        },
    }


def _next_route_record() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": (
            "owner accepts component recombination planning but does not approve "
            "observation or execution"
        ),
        "owner_decision": OWNER_DECISION,
        "recombination_plan_approved": True,
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
        "json_path": str(output_root / "owner_review_decision.json"),
        "component_recombination_decision_json": str(
            output_root / "component_recombination_decision.json"
        ),
        "recombination_principles_json": str(
            output_root / "recombination_principles.json"
        ),
        "next_route_json": str(output_root / "next_route.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_component_ablation_owner_review_decision.md"
        ),
        "component_recombination_decision_markdown": str(
            docs_root / "dynamic_strategy_component_recombination_decision.md"
        ),
        "recombination_principles_markdown": str(
            docs_root / "dynamic_strategy_recombination_principles.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2395_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["component_recombination_decision_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_component_recombination_decision",
            "schema_version": "dynamic_strategy_component_recombination_decision.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "component_recombination_decision": payload.get(
                "component_recombination_decision"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["recombination_principles_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_principles",
            "schema_version": "dynamic_strategy_recombination_principles.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "recombination_principles": payload.get("recombination_principles"),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["next_route_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_component_recombination_next_route",
            "schema_version": "dynamic_strategy_component_recombination_next_route.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "next_route": payload.get("next_route"),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["component_recombination_decision_markdown"]),
        _recombination_decision_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["recombination_principles_markdown"]),
        _recombination_principles_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略组件消融 owner review 与 recombination decision",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- best reusable component：`{payload.get('best_reusable_component')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Executive summary",
            "",
            "TRADING-2394 只记录 owner review decision：采纳 growth_tilt_engine "
            "作为 return engine，采纳 lower_turnover_guardrail 作为 guardrail，"
            "保留 guarded_turnover_transfer 为 owner-review component，并批准进入 "
            "TRADING-2395 recombination candidate plan。本任务不批准 observation、"
            "paper-shadow、scheduler、event append、outcome binding、production 或 broker。",
            "",
            "## Source findings from TRADING-2393",
            "",
            "```json",
            _json_block(payload.get("source_findings")),
            "```",
            "",
            "## Component owner review",
            "",
            "```json",
            _json_block(payload.get("owner_decision_items")),
            "```",
            "",
            "## Growth tilt engine decision",
            "",
            "- 是否作为收益引擎采纳：`True`",
            "- 依据：2393 best reusable component=`growth_tilt_engine`。",
            "",
            "## Lower-turnover guardrail decision",
            "",
            "- 是否只作为 guardrail：`True`",
            "- 依据：2393 decision=`USE_ONLY_AS_GUARDRAIL`。",
            "",
            "## Guarded turnover transfer decision",
            "",
            "- 是否继续 owner review：`True`",
            "- 依据：2393 decision=`OWNER_REVIEW_REQUIRED`。",
            "",
            "## Observation non-approval record",
            "",
            "```json",
            _json_block(payload.get("observation_non_approval_record")),
            "```",
            "",
            "## Recombination decision",
            "",
            "```json",
            _json_block(payload.get("component_recombination_decision")),
            "```",
            "",
            "## Recombination principles",
            "",
            "```json",
            _json_block(payload.get("recombination_principles")),
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


def _recombination_decision_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 component recombination decision",
            "",
            f"- status：`{payload.get('status')}`",
            f"- recombination plan approved：`{payload.get('recombination_plan_approved')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            "",
            "```json",
            _json_block(payload.get("component_recombination_decision")),
            "```",
        ]
    )


def _recombination_principles_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 recombination principles",
            "",
            "```json",
            _json_block(payload.get("recombination_principles")),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 TRADING-2395 路由",
            "",
            f"- status：`{payload.get('status')}`",
            f"- 推荐下一路由：`{payload.get('recommended_next_research_task')}`",
            "- 下一步：component recombination candidate plan",
            "- observation approved：`False`",
            "- paper-shadow enabled：`False`",
            "- production enabled：`False`",
            "- broker action enabled：`False`",
            "",
            "TRADING-2395 只能设计 recombination candidates；2394 不批准回测执行、"
            "observation、paper-shadow、scheduler、production 或 broker。",
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
        "owner_review_decision_2391",
        "component_attribution_plan_2392",
        "ablation_retest_result_2393",
        "component_attribution_matrix_2393",
        "reusable_component_decision_2393",
        "decision_update_2393",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_block(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
