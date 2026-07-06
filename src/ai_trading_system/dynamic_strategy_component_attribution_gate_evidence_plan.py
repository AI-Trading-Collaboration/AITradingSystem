from __future__ import annotations

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
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification import (
    COMPONENT_VALUE_CANDIDATES,
    CURRENT_BEST_PREVIEW_DECISION,
    CURRENT_BEST_PREVIOUS_DECISION,
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification import (
    READY_STATUS as SOURCE_2390_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT,
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    READY_STATUS as SOURCE_2386_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_report_common import (
    json_block as _json_block,
)
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_empty as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2392"
TASK_REGISTER_ID = (
    "TRADING-2392_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN"
)
REPORT_TYPE = "dynamic_strategy_component_attribution_gate_evidence_plan"
SCHEMA_VERSION = "dynamic_strategy_component_attribution_and_gate_evidence_plan.v1"
READY_STATUS = "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2393_Dynamic_Strategy_Component_Attribution_Targeted_Ablation_Retest"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_COMPONENT_PLAN_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2386",
    "TRADING-2390",
    "TRADING-2391",
)
COMPONENTS_TO_ATTRIBUTE: tuple[str, ...] = (
    "turnover_budgeting",
    "valid_until_strictness",
    "growth_tilt_engine",
    "lower_turnover_guardrail",
    "guarded_turnover_transfer",
)
COMPONENT_QUESTIONS: tuple[str, ...] = (
    "what_problem_does_this_component_solve",
    "why_did_the_source_candidate_fail",
    "component_classification",
    "can_it_independently_add_return",
    "does_it_mainly_reduce_risk_or_cost",
    "does_it_conflict_with_valid_until_window",
    "does_it_increase_turnover",
    "does_it_improve_time_or_regime_slice",
    "should_it_enter_ablation_retest",
    "target_candidate_or_signal_family_for_reuse",
)
COMPONENT_CLASSES: dict[str, str] = {
    "RETURN_ENGINE": "primarily contributes return convexity or upside capture",
    "RISK_GUARDRAIL": "primarily limits drawdown or high-volatility fragility",
    "EXECUTION_GUARDRAIL": "primarily reduces turnover, cost, lag or stale execution",
    "SIGNAL_FILTER": "primarily filters noisy signals or confirms trend state",
    "COMPONENT_VALUE_ONLY": "source candidate fails but the component remains useful",
    "NOT_REUSABLE": "component evidence is not strong enough for reuse",
}
EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "candidate_auto_accept",
    "research_only_observation_for_candidate",
    "paper_shadow",
    "paper_trade",
    "shadow_position",
    "event_append",
    "outcome_binding",
    "scheduler",
    "daily_report",
    "production",
    "broker_order",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "event_append_approved",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "outcome_store_mutated",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_trade_created",
    "shadow_position_created",
    "production_enabled",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "observation_approved",
    "research_only_observation_approved",
    "candidate_auto_accept_approved",
    "current_best_candidate_observation_approved",
    "paper_shadow_allowed",
    "production_allowed",
    "policy_update_applied",
    "rules_mutated",
    "new_signal_generated",
    "scoring_run",
)

DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT / "candidate_ranking.json"
)
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_ranking.json"
)
DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "reclassification_result.json"
)
DEFAULT_SOURCE_2390_COMPONENT_ATTRIBUTION_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "component_attribution_review.json"
)
DEFAULT_SOURCE_2390_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "candidate_reclassification_preview.json"
)
DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2391_CANDIDATE_OWNER_REVIEW_RECORD_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "candidate_owner_review_record.json"
)
DEFAULT_SOURCE_2391_OBSERVATION_NON_APPROVAL_RECORD_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "observation_non_approval_record.json"
)


def run_dynamic_strategy_component_attribution_gate_evidence_plan(
    *,
    source_candidate_ranking_2365_path: Path = (
        DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH
    ),
    source_sensitivity_result_2366_path: Path = (
        DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH
    ),
    source_expanded_candidate_retest_2386_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_expanded_candidate_ranking_2386_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH
    ),
    source_reclassification_result_2390_path: Path = (
        DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH
    ),
    source_component_attribution_review_2390_path: Path = (
        DEFAULT_SOURCE_2390_COMPONENT_ATTRIBUTION_REVIEW_PATH
    ),
    source_candidate_reclassification_preview_2390_path: Path = (
        DEFAULT_SOURCE_2390_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH
    ),
    source_owner_review_decision_2391_path: Path = (
        DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH
    ),
    source_candidate_owner_review_record_2391_path: Path = (
        DEFAULT_SOURCE_2391_CANDIDATE_OWNER_REVIEW_RECORD_PATH
    ),
    source_observation_non_approval_record_2391_path: Path = (
        DEFAULT_SOURCE_2391_OBSERVATION_NON_APPROVAL_RECORD_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_candidate_ranking_2365_path=source_candidate_ranking_2365_path,
        source_sensitivity_result_2366_path=source_sensitivity_result_2366_path,
        source_expanded_candidate_retest_2386_path=(
            source_expanded_candidate_retest_2386_path
        ),
        source_expanded_candidate_ranking_2386_path=(
            source_expanded_candidate_ranking_2386_path
        ),
        source_reclassification_result_2390_path=(
            source_reclassification_result_2390_path
        ),
        source_component_attribution_review_2390_path=(
            source_component_attribution_review_2390_path
        ),
        source_candidate_reclassification_preview_2390_path=(
            source_candidate_reclassification_preview_2390_path
        ),
        source_owner_review_decision_2391_path=source_owner_review_decision_2391_path,
        source_candidate_owner_review_record_2391_path=(
            source_candidate_owner_review_record_2391_path
        ),
        source_observation_non_approval_record_2391_path=(
            source_observation_non_approval_record_2391_path
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
    sources: dict[str, Any] = {
        **documents,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
        "source_status": {
            key: _as_mapping(document).get("status")
            for key, document in documents.items()
        },
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    sources["source_ready_for_component_plan"] = not sources["source_validation_errors"]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "candidate_ranking_2365": SOURCE_2365_READY_STATUS,
        "sensitivity_result_2366": SOURCE_2366_READY_STATUS,
        "expanded_candidate_retest_2386": SOURCE_2386_READY_STATUS,
        "expanded_candidate_ranking_2386": SOURCE_2386_READY_STATUS,
        "reclassification_result_2390": SOURCE_2390_READY_STATUS,
        "component_attribution_review_2390": SOURCE_2390_READY_STATUS,
        "candidate_reclassification_preview_2390": SOURCE_2390_READY_STATUS,
        "owner_review_decision_2391": SOURCE_2391_READY_STATUS,
        "candidate_owner_review_record_2391": SOURCE_2391_READY_STATUS,
        "observation_non_approval_record_2391": SOURCE_2391_READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )

    _validate_2365_2366_context(sources, errors)
    _validate_2386_candidate_context(sources, errors)
    _validate_2390_component_context(sources, errors)
    _validate_2391_owner_decision(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2365_2366_context(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    ranking = _as_mapping(sources.get("candidate_ranking_2365"))
    ranking_rows = _as_list_of_mappings(ranking.get("candidate_ranking"))
    top_row = next(
        (row for row in ranking_rows if row.get("candidate_id") == RANKING_TOP_CANDIDATE),
        {},
    )
    if not top_row:
        errors.append("2365 candidate ranking missing current best candidate")
    elif top_row.get("decision") not in (
        "OWNER_REVIEW_REQUIRED",
        CURRENT_BEST_PREVIOUS_DECISION,
    ):
        errors.append("2365 current best candidate decision is unexpected")
    sensitivity = _as_mapping(sources.get("sensitivity_result_2366"))
    if sensitivity.get("top_candidate_from_2365") not in (None, RANKING_TOP_CANDIDATE):
        errors.append("2366 sensitivity top candidate does not match current best")


def _validate_2386_candidate_context(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("expanded_candidate_retest_2386"))
    if retest.get("best_candidate_after_expanded_screening") != RANKING_TOP_CANDIDATE:
        errors.append("2386 best candidate is not the current best candidate")
    if retest.get("best_candidate_decision") != CURRENT_BEST_PREVIOUS_DECISION:
        errors.append("2386 best candidate decision is not CONTINUE_OPTIMIZATION")
    if retest.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2386 unexpectedly marked candidate observation-ready")
    ranking_rows = _expanded_ranking_rows(sources)
    present = {str(row.get("candidate_id")) for row in ranking_rows}
    required_candidates = {
        RANKING_TOP_CANDIDATE,
        *COMPONENT_VALUE_CANDIDATES,
        "dynamic_regime_overlay_v0_4_lower_turnover",
        "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
        "equal_risk_growth_tilt_guarded_turnover_v1",
    }
    missing = sorted(required_candidates - present)
    if missing:
        errors.append(f"2386 ranking missing component attribution candidates: {missing}")


def _validate_2390_component_context(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    reclassification = _as_mapping(sources.get("reclassification_result_2390"))
    component_doc = _as_mapping(sources.get("component_attribution_review_2390"))
    preview_doc = _as_mapping(sources.get("candidate_reclassification_preview_2390"))
    if reclassification.get("current_best_candidate") != RANKING_TOP_CANDIDATE:
        errors.append("2390 current best candidate mismatch")
    if reclassification.get("current_best_candidate_previous_decision") != (
        CURRENT_BEST_PREVIOUS_DECISION
    ):
        errors.append("2390 previous decision mismatch")
    if reclassification.get("current_best_candidate_preview_decision") != (
        CURRENT_BEST_PREVIEW_DECISION
    ):
        errors.append("2390 calibrated preview decision mismatch")
    for field in (
        "candidate_auto_accept_approved",
        "research_only_observation_approved",
    ):
        if reclassification.get(field) is True:
            errors.append(f"2390 unexpectedly approved {field}")
    component_candidates = set(
        str(item) for item in reclassification.get("component_value_candidates", [])
    )
    component_candidates.update(
        str(item) for item in component_doc.get("component_value_candidates", [])
    )
    if not set(COMPONENT_VALUE_CANDIDATES).issubset(component_candidates):
        errors.append("2390 missing required component value candidates")
    component_names = {
        str(row.get("component_name"))
        for row in _as_list_of_mappings(component_doc.get("component_attribution_review"))
    }
    missing_components = sorted(set(COMPONENTS_TO_ATTRIBUTE) - component_names)
    if missing_components:
        errors.append(f"2390 component review missing components: {missing_components}")
    preview_rows = _as_list_of_mappings(
        preview_doc.get("candidate_reclassification_preview")
    )
    current_best_preview = next(
        (row for row in preview_rows if row.get("candidate_id") == RANKING_TOP_CANDIDATE),
        {},
    )
    if current_best_preview.get("preview_decision") != CURRENT_BEST_PREVIEW_DECISION:
        errors.append("2390 preview missing OWNER_REVIEW_REQUIRED current best row")


def _validate_2391_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2391"))
    candidate_doc = _as_mapping(sources.get("candidate_owner_review_record_2391"))
    candidate_record = _as_mapping(candidate_doc.get("candidate_owner_review_record"))
    non_approval_doc = _as_mapping(sources.get("observation_non_approval_record_2391"))
    non_approval = _as_mapping(non_approval_doc.get("observation_non_approval_record"))
    if owner.get("owner_decision") != SOURCE_2391_OWNER_DECISION:
        errors.append("2391 owner decision mismatch")
    if owner.get("recommended_next_research_task") != SOURCE_2391_EXPECTED_ROUTE:
        errors.append("2391 did not route to TRADING-2392")
    if owner.get("component_attribution_continue_recommended") is not True:
        errors.append("2391 did not recommend component attribution continuation")
    for field in (
        "candidate_auto_accept_approved",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
    ):
        if owner.get(field) is True:
            errors.append(f"2391 owner review safety field must be false: {field}")
    if candidate_record.get("owner_review_required_retained") is not True:
        errors.append("2391 candidate record did not retain owner-review-required")
    if non_approval.get("research_only_observation_approved") is True:
        errors.append("2391 non-approval record approved research-only observation")


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
        "source_ready_for_component_plan": bool(
            sources.get("source_ready_for_component_plan")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": "NOT_APPLICABLE_PRIOR_ARTIFACT_ONLY",
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "observe_only": True,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in SAFETY_FALSE_FIELDS},
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    component_value_matrix = _component_value_matrix(sources)
    gate_evidence_plan = _gate_evidence_plan(component_value_matrix)
    ablation_plan = _targeted_ablation_retest_plan()
    component_attribution_plan = _component_attribution_plan(
        component_value_matrix,
        gate_evidence_plan,
        ablation_plan,
    )
    return {
        "owner_decision_from_2391": SOURCE_2391_OWNER_DECISION,
        "component_attribution_plan_ready": True,
        "component_value_matrix_ready": True,
        "gate_evidence_plan_ready": True,
        "targeted_ablation_retest_plan_ready": True,
        "component_value_candidates": list(COMPONENT_VALUE_CANDIDATES),
        "components_to_attribute": list(COMPONENTS_TO_ATTRIBUTE),
        "component_questions": list(COMPONENT_QUESTIONS),
        "component_classes": COMPONENT_CLASSES,
        "component_attribution_plan": component_attribution_plan,
        "component_value_matrix": component_value_matrix,
        "gate_evidence_plan": gate_evidence_plan,
        "targeted_ablation_retest_plan": ablation_plan,
        "component_acceptance_criteria": _component_acceptance_criteria(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "owner_decision_from_2391": None,
        "component_attribution_plan_ready": False,
        "component_value_matrix_ready": False,
        "gate_evidence_plan_ready": False,
        "targeted_ablation_retest_plan_ready": False,
        "component_value_candidates": [],
        "components_to_attribute": list(COMPONENTS_TO_ATTRIBUTE),
        "component_questions": list(COMPONENT_QUESTIONS),
        "component_classes": COMPONENT_CLASSES,
        "component_attribution_plan": {
            "plan_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "component_value_matrix": [],
        "gate_evidence_plan": {},
        "targeted_ablation_retest_plan": {},
        "component_acceptance_criteria": _component_acceptance_criteria(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources),
        "recommended_next_research_task": None,
    }


def _component_attribution_plan(
    component_value_matrix: list[dict[str, Any]],
    gate_evidence_plan: Mapping[str, Any],
    ablation_plan: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "plan_ready": True,
        "plan_type": "COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_ONLY",
        "candidate_level_approval_separated_from_component_evidence": True,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "component_questions": list(COMPONENT_QUESTIONS),
        "component_classes": COMPONENT_CLASSES,
        "component_count": len(component_value_matrix),
        "components_to_attribute": [row["component_name"] for row in component_value_matrix],
        "components_for_targeted_ablation": [
            row["component_name"]
            for row in component_value_matrix
            if row["recommended_2393_action"] == "TARGETED_ABLATION_RETEST"
        ],
        "components_for_guardrail_only_reuse": [
            row["component_name"]
            for row in component_value_matrix
            if row["reuse_mode"] == "GUARDRAIL_ONLY"
        ],
        "components_to_reject": [
            row["component_name"]
            for row in component_value_matrix
            if row["recommended_2393_action"] == "REJECT_COMPONENT_FOR_NOW"
        ],
        "gate_evidence_plan_ready": bool(gate_evidence_plan.get("plan_ready")),
        "targeted_ablation_retest_plan_ready": bool(ablation_plan.get("plan_ready")),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _component_value_matrix(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    ranking_by_candidate = {
        str(row.get("candidate_id")): row for row in _expanded_ranking_rows(sources)
    }
    review_by_component = {
        str(row.get("component_name")): row
        for row in _component_review_rows(sources)
        if row.get("component_name")
    }
    matrix = []
    for spec in _component_specs():
        component_name = str(spec["component_name"])
        review = _as_mapping(review_by_component.get(component_name))
        source_candidates = list(spec["source_candidates"])
        source_rows = [
            ranking_by_candidate[candidate_id]
            for candidate_id in source_candidates
            if candidate_id in ranking_by_candidate
        ]
        candidate_level_status = str(spec["candidate_level_status"])
        reusable = bool(review.get("reusable_in_future_candidate")) or component_name in (
            "growth_tilt_engine",
            "lower_turnover_guardrail",
            "guarded_turnover_transfer",
        )
        matrix.append(
            {
                "component_name": component_name,
                "source_candidates": source_candidates,
                "candidate_level_status": candidate_level_status,
                "component_class": spec["component_class"],
                "secondary_classes": list(spec["secondary_classes"]),
                "possible_component_value": list(spec["possible_component_value"]),
                "problem_solved": spec["problem_solved"],
                "source_candidate_failure_context": _failure_context(
                    component_name,
                    source_rows,
                    review,
                ),
                "can_independently_add_return": bool(spec["can_independently_add_return"]),
                "mainly_reduces_risk_or_cost": bool(spec["mainly_reduces_risk_or_cost"]),
                "valid_until_window_conflict": "NO_KNOWN_CONFLICT_PLAN_TO_VERIFY",
                "turnover_impact_to_measure": spec["turnover_impact_to_measure"],
                "time_regime_slice_impact_to_measure": (
                    "REQUIRES_TARGETED_ABLATION_EVIDENCE"
                ),
                "reusable_in_future_candidate": reusable,
                "targeted_ablation_retest_recommended": True,
                "recommended_2393_action": "TARGETED_ABLATION_RETEST",
                "reuse_mode": spec["reuse_mode"],
                "target_candidate_or_signal_family": (
                    spec["target_candidate_or_signal_family"]
                ),
                "supporting_metrics": _supporting_metrics(source_rows, review),
                "failure_metrics": _failure_metrics(source_rows, review),
                "component_value_only": component_name
                in ("turnover_budgeting", "valid_until_strictness"),
                "candidate_level_approval": False,
            }
        )
    return matrix


def _gate_evidence_plan(
    component_value_matrix: list[dict[str, Any]],
) -> dict[str, Any]:
    evidence_by_component = {
        "turnover_budgeting": {
            "required_future_tests": [
                "turnover_budget_ablation",
                "cost_drag_reduction_test",
                "return_retention_test",
                "max_monthly_turnover_stress",
                "valid_until_window_preservation_check",
            ],
            "success_criteria": [
                "reduces_turnover_vs_source_candidate",
                "does_not_destroy_cost_adjusted_return",
                "improves_or_preserves_realistic_cost_gap",
                "does_not_increase_stale_signal_execution",
            ],
        },
        "valid_until_strictness": {
            "required_future_tests": [
                "stale_signal_prevention_ablation",
                "near_expiry_signal_decay_test",
                "signal_to_execution_lag_test",
                "return_gap_impact_test",
                "regime_slice_impact_test",
            ],
            "success_criteria": [
                "reduces_stale_signal_execution_count",
                "preserves_positive_dynamic_vs_static_gap",
                "does_not_excessively_reduce_upside_capture",
                "improves_signal_expiry_discipline",
            ],
        },
        "growth_tilt_engine": {
            "required_future_tests": [
                "growth_tilt_ablation",
                "risk_on_upside_capture_test",
                "high_volatility_drawdown_test",
                "trend_confirmed_gate_test",
                "drawdown_compensation_test",
            ],
            "success_criteria": [
                "preserves_return_advantage",
                "improves_upside_capture",
                "does_not_materially_worsen_drawdown_after_guardrails",
                "passes_realistic_and_conservative_cost",
            ],
        },
        "lower_turnover_guardrail": {
            "required_future_tests": [
                "lower_turnover_ablation",
                "cooldown_ablation",
                "max_step_weight_delta_test",
                "turnover_cap_stress",
                "return_gap_tradeoff_test",
            ],
            "success_criteria": [
                "reduces_turnover",
                "improves_cost_resilience",
                "does_not_destroy_return_gap_too_much",
                "preserves_valid_until_window",
            ],
        },
        "guarded_turnover_transfer": {
            "required_future_tests": [
                "guarded_transfer_ablation",
                "ranking_top_return_retention_test",
                "drawdown_fragility_test",
                "turnover_tradeoff_test",
            ],
            "success_criteria": [
                "reduces_fragility_relative_to_ranking_top",
                "keeps_return_gap_close_to_ranking_top",
                "does_not_add_stale_signal_execution",
                "preserves_valid_until_window",
            ],
        },
    }
    return {
        "plan_ready": True,
        "component_gate_evidence": [
            {
                "component_name": row["component_name"],
                **evidence_by_component[row["component_name"]],
                "candidate_level_approval_required_after_evidence": True,
                "paper_shadow_gate_separate": True,
                "execution_paths_enabled": False,
            }
            for row in component_value_matrix
        ],
    }


def _targeted_ablation_retest_plan() -> dict[str, Any]:
    ablation_candidates = [
        {
            "candidate_id": "growth_tilt_only_reference",
            "base": RANKING_TOP_CANDIDATE,
            "remove": ["guarded_turnover", "strict_valid_until", "turnover_budget"],
            "add": [],
            "purpose": "measure raw growth tilt engine",
        },
        {
            "candidate_id": "growth_tilt_plus_turnover_budget",
            "base": RANKING_TOP_CANDIDATE,
            "remove": [],
            "add": ["turnover_budgeting"],
            "purpose": "test whether turnover budgeting improves execution without killing return",
        },
        {
            "candidate_id": "growth_tilt_plus_valid_until_strict",
            "base": RANKING_TOP_CANDIDATE,
            "remove": [],
            "add": ["valid_until_strictness"],
            "purpose": "test whether strict expiry improves stale signal control",
        },
        {
            "candidate_id": "growth_tilt_plus_turnover_budget_and_valid_until",
            "base": RANKING_TOP_CANDIDATE,
            "remove": [],
            "add": ["turnover_budgeting", "valid_until_strictness"],
            "purpose": "test combined component transfer",
        },
        {
            "candidate_id": "lower_turnover_without_cooldown",
            "base": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
            "remove": ["cooldown_balancing"],
            "add": [],
            "purpose": "measure cooldown contribution",
        },
        {
            "candidate_id": "lower_turnover_plus_growth_tilt_component",
            "base": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
            "remove": [],
            "add": ["guarded_growth_tilt_engine"],
            "purpose": (
                "test whether lower-turnover reference can gain upside without "
                "losing robustness"
            ),
        },
    ]
    return {
        "plan_ready": True,
        "target_task": NEXT_ROUTE,
        "plan_type": "TARGETED_ABLATION_RETEST_PLAN_FOR_2393",
        "ablation_test_candidates": ablation_candidates,
        "acceptance_criteria": _component_acceptance_criteria(),
        "must_not_approve_observation": True,
        "must_run_data_quality_gate_if_2393_reads_cached_market_data": True,
        "scheduler_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }


def _component_acceptance_criteria() -> dict[str, Any]:
    return {
        "reusable_component": {
            "must": [
                "improves_one_target_metric",
                "does_not_materially_worsen_core_guardrail",
                "preserves_valid_until_window",
                "does_not_require_scheduler_or_paper_shadow",
            ]
        },
        "component_to_reject": {
            "condition": [
                "no_clear_metric_improvement",
                "or_worsens_drawdown_without_return_compensation",
                "or_increases_turnover_without_cost_adjusted_return_gain",
                "or_increases_stale_signal_execution",
            ]
        },
        "component_owner_review_required": {
            "condition": [
                "improves_return_or_risk_but_tradeoff_requires_human_judgment"
            ]
        },
    }


def _component_specs() -> list[dict[str, Any]]:
    return [
        {
            "component_name": "turnover_budgeting",
            "source_candidates": ["dynamic_turnover_budgeted_growth_tilt_v1"],
            "candidate_level_status": "COMPONENT_VALUE_ONLY",
            "component_class": "EXECUTION_GUARDRAIL",
            "secondary_classes": ["COMPONENT_VALUE_ONLY"],
            "possible_component_value": [
                "explicit turnover budget",
                "cost-aware growth tilt",
                "turnover discipline",
            ],
            "problem_solved": "reduce turnover and cost drag without deleting growth tilt",
            "can_independently_add_return": False,
            "mainly_reduces_risk_or_cost": True,
            "turnover_impact_to_measure": "SHOULD_REDUCE_OR_BUDGET_TURNOVER",
            "reuse_mode": "GUARDRAIL_ONLY",
            "target_candidate_or_signal_family": RANKING_TOP_CANDIDATE,
        },
        {
            "component_name": "valid_until_strictness",
            "source_candidates": ["dynamic_valid_until_expiry_strict_v1"],
            "candidate_level_status": "COMPONENT_VALUE_ONLY",
            "component_class": "EXECUTION_GUARDRAIL",
            "secondary_classes": ["SIGNAL_FILTER", "COMPONENT_VALUE_ONLY"],
            "possible_component_value": [
                "strict signal expiry",
                "stale signal prevention",
                "near-expiry risk control",
            ],
            "problem_solved": "prevent stale signal carry-forward and near-expiry execution",
            "can_independently_add_return": False,
            "mainly_reduces_risk_or_cost": True,
            "turnover_impact_to_measure": "MAY_REDUCE_STALE_ACTIONS_BUT_CAN_LOWER_UPSIDE",
            "reuse_mode": "GUARDRAIL_ONLY",
            "target_candidate_or_signal_family": RANKING_TOP_CANDIDATE,
        },
        {
            "component_name": "growth_tilt_engine",
            "source_candidates": [RANKING_TOP_CANDIDATE],
            "candidate_level_status": "OWNER_REVIEW_REQUIRED_PREVIEW",
            "component_class": "RETURN_ENGINE",
            "secondary_classes": [],
            "possible_component_value": [
                "return advantage",
                "upside capture",
                "risk-on responsiveness",
            ],
            "problem_solved": "retain the return engine that ranks above static baseline",
            "can_independently_add_return": True,
            "mainly_reduces_risk_or_cost": False,
            "turnover_impact_to_measure": "CAN_RELY_ON_HIGHER_TURNOVER",
            "reuse_mode": "RETURN_ENGINE_WITH_GUARDRAILS",
            "target_candidate_or_signal_family": RANKING_TOP_CANDIDATE,
        },
        {
            "component_name": "lower_turnover_guardrail",
            "source_candidates": [
                "dynamic_regime_overlay_v0_4_lower_turnover",
                "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
            ],
            "candidate_level_status": "GUARDRAIL_REFERENCE_ONLY",
            "component_class": "EXECUTION_GUARDRAIL",
            "secondary_classes": ["RISK_GUARDRAIL"],
            "possible_component_value": [
                "turnover reduction",
                "cost stress resilience",
                "execution discipline",
            ],
            "problem_solved": "lower execution churn and cost stress exposure",
            "can_independently_add_return": False,
            "mainly_reduces_risk_or_cost": True,
            "turnover_impact_to_measure": "SHOULD_REDUCE_TURNOVER",
            "reuse_mode": "GUARDRAIL_ONLY",
            "target_candidate_or_signal_family": RANKING_TOP_CANDIDATE,
        },
        {
            "component_name": "guarded_turnover_transfer",
            "source_candidates": ["equal_risk_growth_tilt_guarded_turnover_v1"],
            "candidate_level_status": "GUARDRAIL_TRANSFER_REFERENCE_ONLY",
            "component_class": "RISK_GUARDRAIL",
            "secondary_classes": ["EXECUTION_GUARDRAIL"],
            "possible_component_value": [
                "partial transfer of lower-turnover guardrail to ranking top",
                "reduced fragility relative to original ranking top",
            ],
            "problem_solved": "reduce ranking-top fragility while retaining return engine",
            "can_independently_add_return": False,
            "mainly_reduces_risk_or_cost": True,
            "turnover_impact_to_measure": "SHOULD_LOWER_FRAGILITY_AND_CONTROL_TURNOVER",
            "reuse_mode": "GUARDRAIL_TRANSFER",
            "target_candidate_or_signal_family": RANKING_TOP_CANDIDATE,
        },
    ]


def _failure_context(
    component_name: str,
    source_rows: list[dict[str, Any]],
    review: Mapping[str, Any],
) -> list[str]:
    review_failure = _as_list_of_mappings(review.get("failure_metrics"))
    contexts = []
    for row in source_rows:
        reasons = row.get("decision_reasons")
        if isinstance(reasons, list):
            contexts.extend(str(item) for item in reasons[:4])
        elif row.get("decision_reason"):
            contexts.append(str(row.get("decision_reason")))
    for failure in review_failure:
        for key in (
            "time_slice_pass_rate",
            "regime_slice_pass_rate",
            "drawdown_not_materially_worse",
            "return_advantage_retained",
            "candidate_vs_ranking_top_gap",
        ):
            if key in failure:
                contexts.append(f"{key}={failure.get(key)}")
    if contexts:
        return contexts[:8]
    return [f"{component_name} requires targeted ablation evidence before reuse"]


def _supporting_metrics(
    source_rows: list[dict[str, Any]],
    review: Mapping[str, Any],
) -> list[dict[str, Any]]:
    review_supporting = _as_list_of_mappings(review.get("supporting_metrics"))
    if review_supporting:
        return review_supporting
    return [
        {
            "candidate_id": row.get("candidate_id"),
            "dynamic_vs_static_gap": row.get("dynamic_vs_static_gap"),
            "cost_adjusted_dynamic_vs_static_gap": row.get(
                "cost_adjusted_dynamic_vs_static_gap"
            ),
            "turnover": row.get("turnover"),
            "turnover_budget_passed": row.get("turnover_budget_passed"),
            "valid_until_window_preserved": row.get("valid_until_window_preserved"),
            "no_stale_signal_carry_forward": row.get("no_stale_signal_carry_forward"),
        }
        for row in source_rows
    ]


def _failure_metrics(
    source_rows: list[dict[str, Any]],
    review: Mapping[str, Any],
) -> list[dict[str, Any]]:
    review_failure = _as_list_of_mappings(review.get("failure_metrics"))
    if review_failure:
        return review_failure
    return [
        {
            "candidate_id": row.get("candidate_id"),
            "drawdown_gap_vs_static": row.get("drawdown_gap_vs_static"),
            "time_slice_pass_rate": row.get("time_slice_pass_rate"),
            "regime_slice_pass_rate": row.get("regime_slice_pass_rate"),
            "candidate_vs_ranking_top_gap": row.get("candidate_vs_ranking_top_gap"),
            "return_advantage_retained": row.get("return_advantage_retained"),
        }
        for row in source_rows
    ]


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking = _as_mapping(sources.get("candidate_ranking_2365"))
    sensitivity = _as_mapping(sources.get("sensitivity_result_2366"))
    retest = _as_mapping(sources.get("expanded_candidate_retest_2386"))
    reclassification = _as_mapping(sources.get("reclassification_result_2390"))
    owner = _as_mapping(sources.get("owner_review_decision_2391"))
    return {
        "trading_2365": {
            "status": ranking.get("status"),
            "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        },
        "trading_2366": {
            "status": sensitivity.get("status"),
            "top_candidate_from_2365": sensitivity.get("top_candidate_from_2365"),
        },
        "trading_2386": {
            "status": retest.get("status"),
            "best_candidate_after_expanded_screening": retest.get(
                "best_candidate_after_expanded_screening"
            ),
            "best_candidate_decision": retest.get("best_candidate_decision"),
        },
        "trading_2390": {
            "status": reclassification.get("status"),
            "component_value_candidates": reclassification.get(
                "component_value_candidates"
            ),
            "current_best_candidate_preview_decision": reclassification.get(
                "current_best_candidate_preview_decision"
            ),
        },
        "trading_2391": {
            "status": owner.get("status"),
            "owner_decision": owner.get("owner_decision"),
            "component_attribution_continue_recommended": owner.get(
                "component_attribution_continue_recommended"
            ),
            "research_only_observation_approved": owner.get(
                "research_only_observation_approved"
            ),
        },
    }


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_ONLY",
        "ablation_retest_executed": False,
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


def _expanded_ranking_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    ranking = _as_mapping(sources.get("expanded_candidate_ranking_2386"))
    rows = _as_list_of_mappings(ranking.get("expanded_candidate_ranking"))
    if rows:
        return rows
    retest = _as_mapping(sources.get("expanded_candidate_retest_2386"))
    return _as_list_of_mappings(retest.get("expanded_candidate_ranking"))


def _component_review_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    component_doc = _as_mapping(sources.get("component_attribution_review_2390"))
    rows = _as_list_of_mappings(component_doc.get("component_attribution_review"))
    if rows:
        return rows
    reclassification = _as_mapping(sources.get("reclassification_result_2390"))
    return _as_list_of_mappings(reclassification.get("component_attribution_review"))


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "json_path": str(output_root / "component_attribution_plan.json"),
        "component_value_matrix_json": str(output_root / "component_value_matrix.json"),
        "gate_evidence_plan_json": str(output_root / "gate_evidence_plan.json"),
        "targeted_ablation_retest_plan_json": str(
            output_root / "targeted_ablation_retest_plan.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_component_attribution_gate_evidence_plan.md"
        ),
        "component_value_matrix_markdown": str(
            docs_root / "dynamic_strategy_component_value_matrix.md"
        ),
        "gate_evidence_plan_markdown": str(
            docs_root / "dynamic_strategy_gate_evidence_plan.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2393_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["component_value_matrix_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_component_value_matrix",
            "schema_version": "dynamic_strategy_component_value_matrix.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "component_value_matrix": payload.get("component_value_matrix", []),
            "component_value_candidates": payload.get("component_value_candidates", []),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["gate_evidence_plan_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_gate_evidence_plan",
            "schema_version": "dynamic_strategy_gate_evidence_plan.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "gate_evidence_plan": payload.get("gate_evidence_plan", {}),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["targeted_ablation_retest_plan_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_targeted_ablation_retest_plan",
            "schema_version": "dynamic_strategy_targeted_ablation_retest_plan.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "targeted_ablation_retest_plan": payload.get(
                "targeted_ablation_retest_plan", {}
            ),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["component_value_matrix_markdown"]),
        _component_value_matrix_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["gate_evidence_plan_markdown"]),
        _gate_evidence_plan_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略组件归因与门禁证据计划",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as_of：`{payload.get('as_of')}`",
            f"- 2391 owner decision：`{payload.get('owner_decision_from_2391')}`",
            f"- 下一路由：`{payload.get('recommended_next_research_task')}`",
            "",
            "## 执行摘要",
            "",
            (
                "TRADING-2392 将组件级证据与候选级批准分离：本任务只定义组件归因、"
                "门禁证据和后续 targeted ablation retest 计划，不运行 ablation retest，"
                "也不批准 observation、paper-shadow、production 或 broker 动作。"
            ),
            "",
            "## TRADING-2390 / TRADING-2391 来源结论",
            "",
            "```json",
            _json_block(payload.get("source_findings")),
            "```",
            "",
            "## 组件归因范围",
            "",
            _component_scope_table(payload.get("component_value_matrix", [])),
            "",
            "## 组件价值矩阵",
            "",
            "```json",
            _json_block(payload.get("component_value_matrix")),
            "```",
            "",
            "## 门禁证据计划",
            "",
            "```json",
            _json_block(payload.get("gate_evidence_plan")),
            "```",
            "",
            "## targeted ablation retest 计划",
            "",
            "```json",
            _json_block(payload.get("targeted_ablation_retest_plan")),
            "```",
            "",
            "## 组件验收标准",
            "",
            "```json",
            _json_block(payload.get("component_acceptance_criteria")),
            "```",
            "",
            "## 明确未批准事项",
            "",
            "\n".join(
                f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])
            ),
            "",
            "## 安全边界摘要",
            "",
            "```json",
            _json_block(payload.get("guardrail_summary")),
            "```",
        ]
    )


def _component_value_matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略组件价值矩阵",
            "",
            f"- status：`{payload.get('status')}`",
            "- observation approved：`False`",
            "",
            _component_scope_table(payload.get("component_value_matrix", [])),
            "",
            "```json",
            _json_block(payload.get("component_value_matrix")),
            "```",
        ]
    )


def _gate_evidence_plan_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略门禁证据计划",
            "",
            f"- status：`{payload.get('status')}`",
            "- ablation retest executed：`False`",
            "- paper-shadow enabled：`False`",
            "",
            "```json",
            _json_block(payload.get("gate_evidence_plan")),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 TRADING-2393 路由",
            "",
            f"- status：`{payload.get('status')}`",
            f"- 推荐下一路由：`{payload.get('recommended_next_research_task')}`",
            "- 下一步：component attribution targeted ablation retest",
            "- TRADING-2392 是否执行 ablation retest：`False`",
            "- observation approved：`False`",
            "- paper-shadow enabled：`False`",
            "- production enabled：`False`",
            "- broker action enabled：`False`",
            "",
            (
                "TRADING-2393 只有在读取 cached market data 时先执行必需的数据质量门禁后，"
                "才允许运行 targeted ablation retest。"
            ),
        ]
    )


def _component_scope_table(rows: Any) -> str:
    items = _as_list_of_mappings(rows)
    lines = [
        "|组件|类别|来源候选|候选级状态|2393 动作|",
        "|---|---|---|---|---|",
    ]
    for row in items:
        candidates = ", ".join(
            f"`{candidate}`" for candidate in row.get("source_candidates", [])
        )
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{row.get('component_name')}`",
                    f"`{row.get('component_class')}`",
                    candidates,
                    f"`{row.get('candidate_level_status')}`",
                    f"`{row.get('recommended_2393_action')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _source_document_names() -> tuple[str, ...]:
    return (
        "candidate_ranking_2365",
        "sensitivity_result_2366",
        "expanded_candidate_retest_2386",
        "expanded_candidate_ranking_2386",
        "reclassification_result_2390",
        "component_attribution_review_2390",
        "candidate_reclassification_preview_2390",
        "owner_review_decision_2391",
        "candidate_owner_review_record_2391",
        "observation_non_approval_record_2391",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]
