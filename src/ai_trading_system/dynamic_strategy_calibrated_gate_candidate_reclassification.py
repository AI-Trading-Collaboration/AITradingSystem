from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision import (
    NEXT_ROUTE as SOURCE_2389_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision import (
    READY_STATUS as SOURCE_2389_READY_STATUS,
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
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.dynamic_strategy_research_filter_threshold_methodology_review import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_filter_threshold_methodology_review import (
    READY_STATUS as SOURCE_2388_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2390"
TASK_REGISTER_ID = (
    "TRADING-2390_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_"
    "RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION"
)
REPORT_TYPE = "dynamic_strategy_calibrated_gate_candidate_reclassification"
SCHEMA_VERSION = (
    "dynamic_strategy_calibrated_gate_candidate_reclassification_and_"
    "component_attribution.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_"
    "COMPONENT_ATTRIBUTION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_"
    "COMPONENT_ATTRIBUTION_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2391_Dynamic_Strategy_Calibrated_Gate_Candidate_Owner_Review_"
    "And_Observation_Decision"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_RECLASSIFICATION_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2386",
    "TRADING-2388",
    "TRADING-2389",
)
CURRENT_BEST_PREVIOUS_DECISION = "CONTINUE_OPTIMIZATION"
CURRENT_BEST_PREVIEW_DECISION = "OWNER_REVIEW_REQUIRED"
COMPONENT_VALUE_DECISION = "COMPONENT_VALUE_ONLY"
CONTINUE_OPTIMIZATION_DECISION = "CONTINUE_OPTIMIZATION"
REJECT_FOR_NOW_DECISION = "REJECT_FOR_NOW"
DEPRECATED_BY_CALIBRATED_GATE_DECISION = "DEPRECATED_BY_CALIBRATED_GATE"

COMPONENT_VALUE_CANDIDATES: tuple[str, ...] = (
    "dynamic_turnover_budgeted_growth_tilt_v1",
    "dynamic_valid_until_expiry_strict_v1",
)
REQUIRED_FOCUS_CANDIDATES: tuple[str, ...] = (
    RANKING_TOP_CANDIDATE,
    *COMPONENT_VALUE_CANDIDATES,
    "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
    "equal_risk_growth_tilt_guarded_turnover_v1",
)
FOCUS_CANDIDATE_ROLES: dict[str, str] = {
    RANKING_TOP_CANDIDATE: "current_best_reference_candidate",
    "dynamic_turnover_budgeted_growth_tilt_v1": "turnover_budget_component_case",
    "dynamic_valid_until_expiry_strict_v1": "valid_until_component_case",
    "dynamic_regime_overlay_v0_4_cooldown_balanced_v1": "robustness_reference",
    "equal_risk_growth_tilt_guarded_turnover_v1": "guarded_return_reference",
}
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
    "paper_shadow_allowed",
    "production_allowed",
    "policy_update_applied",
    "rules_mutated",
)

DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_DOCS_ROOT = (
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
DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "threshold_methodology_review_result.json"
)
DEFAULT_SOURCE_2388_CANDIDATE_THRESHOLD_OUTCOME_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "candidate_threshold_outcome_matrix.json"
)
DEFAULT_SOURCE_2388_RECOMMENDED_GATE_POLICY_PROPOSAL_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "recommended_gate_policy_proposal.json"
)
DEFAULT_SOURCE_2389_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2389_CALIBRATED_GATE_ADOPTION_RECORD_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "calibrated_gate_adoption_record.json"
)
DEFAULT_SOURCE_2389_NON_APPROVAL_RECORD_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "non_approval_record.json"
)
DEFAULT_SOURCE_2389_NEXT_RECLASSIFICATION_ROUTE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "next_reclassification_route.json"
)


def run_dynamic_strategy_calibrated_gate_candidate_reclassification(
    *,
    source_candidate_ranking_2365_path: Path = (
        DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH
    ),
    source_sensitivity_result_2366_path: Path = (
        DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH
    ),
    source_expanded_candidate_retest_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_expanded_candidate_ranking_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH
    ),
    source_expanded_decision_update_path: Path = (
        DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH
    ),
    source_threshold_methodology_review_path: Path = (
        DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH
    ),
    source_candidate_threshold_outcome_matrix_path: Path = (
        DEFAULT_SOURCE_2388_CANDIDATE_THRESHOLD_OUTCOME_MATRIX_PATH
    ),
    source_recommended_gate_policy_proposal_path: Path = (
        DEFAULT_SOURCE_2388_RECOMMENDED_GATE_POLICY_PROPOSAL_PATH
    ),
    source_owner_review_decision_path: Path = (
        DEFAULT_SOURCE_2389_OWNER_REVIEW_DECISION_PATH
    ),
    source_calibrated_gate_adoption_record_path: Path = (
        DEFAULT_SOURCE_2389_CALIBRATED_GATE_ADOPTION_RECORD_PATH
    ),
    source_non_approval_record_path: Path = (
        DEFAULT_SOURCE_2389_NON_APPROVAL_RECORD_PATH
    ),
    source_next_reclassification_route_path: Path = (
        DEFAULT_SOURCE_2389_NEXT_RECLASSIFICATION_ROUTE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_candidate_ranking_2365_path=source_candidate_ranking_2365_path,
        source_sensitivity_result_2366_path=source_sensitivity_result_2366_path,
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_expanded_decision_update_path=source_expanded_decision_update_path,
        source_threshold_methodology_review_path=(
            source_threshold_methodology_review_path
        ),
        source_candidate_threshold_outcome_matrix_path=(
            source_candidate_threshold_outcome_matrix_path
        ),
        source_recommended_gate_policy_proposal_path=(
            source_recommended_gate_policy_proposal_path
        ),
        source_owner_review_decision_path=source_owner_review_decision_path,
        source_calibrated_gate_adoption_record_path=(
            source_calibrated_gate_adoption_record_path
        ),
        source_non_approval_record_path=source_non_approval_record_path,
        source_next_reclassification_route_path=source_next_reclassification_route_path,
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
    sources["source_ready_for_candidate_reclassification"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "candidate_ranking_2365": SOURCE_2365_READY_STATUS,
        "sensitivity_result_2366": SOURCE_2366_READY_STATUS,
        "expanded_candidate_retest": SOURCE_2386_READY_STATUS,
        "expanded_candidate_ranking": SOURCE_2386_READY_STATUS,
        "expanded_decision_update": SOURCE_2386_READY_STATUS,
        "threshold_methodology_review": SOURCE_2388_READY_STATUS,
        "candidate_threshold_outcome_matrix": SOURCE_2388_READY_STATUS,
        "recommended_gate_policy_proposal": SOURCE_2388_READY_STATUS,
        "owner_review_decision": SOURCE_2389_READY_STATUS,
        "calibrated_gate_adoption_record": SOURCE_2389_READY_STATUS,
        "non_approval_record": SOURCE_2389_READY_STATUS,
        "next_reclassification_route": SOURCE_2389_READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )

    _validate_2389_owner_decision(sources, errors)
    _validate_2388_methodology(sources, errors)
    _validate_2386_candidate_sources(sources, errors)
    _validate_2365_2366_context(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2389_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision"))
    adoption_doc = _as_mapping(sources.get("calibrated_gate_adoption_record"))
    adoption = _as_mapping(adoption_doc.get("calibrated_gate_adoption_record"))
    non_approval_doc = _as_mapping(sources.get("non_approval_record"))
    non_approval = _as_mapping(non_approval_doc.get("non_approval_record"))
    route_doc = _as_mapping(sources.get("next_reclassification_route"))
    route = _as_mapping(route_doc.get("next_reclassification_route"))

    if owner.get("owner_decision") != OWNER_DECISION:
        errors.append("2389 owner_decision does not match adopted calibrated gate decision")
    if owner.get("recommended_next_research_task") != SOURCE_2389_EXPECTED_ROUTE:
        errors.append("2389 owner review does not route to TRADING-2390")
    if owner.get("reference_candidate_policy_adopted") != (
        REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2389 reference candidate policy was not adopted")
    for field in (
        "threshold_methodology_adopted",
        "research_only_vs_paper_shadow_gate_separated",
        "calibrated_reclassification_preview_approved",
        "component_attribution_review_required",
        "future_statistical_threshold_calibration_required",
    ):
        if owner.get(field) is not True:
            errors.append(f"2389 owner review missing required true flag {field}")
    for field in (
        "candidate_auto_accept_approved",
        "current_best_candidate_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
    ):
        if owner.get(field) is True:
            errors.append(f"2389 owner review safety field must be false: {field}")

    policy = _as_mapping(adoption.get("reference_candidate_policy"))
    if policy.get("adopted_policy") != REFERENCE_CANDIDATE_POLICY_RECOMMENDATION:
        errors.append("2389 adoption record missing reference candidate policy")
    if adoption.get("threshold_methodology_adopted") is not True:
        errors.append("2389 adoption record did not adopt threshold methodology")
    if non_approval.get("current_best_candidate_observation_approved") is True:
        errors.append("2389 non-approval record approved current best observation")
    if route.get("recommended_next_research_task") != SOURCE_2389_EXPECTED_ROUTE:
        errors.append("2389 route artifact does not route to TRADING-2390")


def _validate_2388_methodology(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    methodology = _as_mapping(sources.get("threshold_methodology_review"))
    proposal_doc = _as_mapping(sources.get("recommended_gate_policy_proposal"))
    proposal = _as_mapping(proposal_doc.get("recommended_gate_policy_proposal"))
    matrix_rows = _candidate_threshold_rows(sources)

    if methodology.get("threshold_methodology_review_ready") is not True:
        errors.append("2388 threshold_methodology_review_ready is not true")
    if methodology.get("research_only_vs_paper_shadow_gate_separated") is not True:
        errors.append("2388 did not separate research-only and paper-shadow gates")
    if methodology.get("reference_candidate_policy_recommendation") != (
        REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2388 methodology reference policy recommendation mismatch")
    proposal_policy = _as_mapping(proposal.get("reference_candidate_policy"))
    if proposal_policy.get("recommended") != REFERENCE_CANDIDATE_POLICY_RECOMMENDATION:
        errors.append("2388 recommended gate proposal reference policy mismatch")
    if proposal.get("policy_update_applied") is True or proposal.get("rules_mutated") is True:
        errors.append("2388 proposal must not have applied policy or mutated rules")

    present = {str(row.get("candidate_id")) for row in matrix_rows}
    for candidate_id in REQUIRED_FOCUS_CANDIDATES:
        if candidate_id not in present:
            errors.append(f"2388 matrix missing focus candidate {candidate_id}")


def _validate_2386_candidate_sources(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("expanded_candidate_retest"))
    decision = _as_mapping(sources.get("expanded_decision_update"))
    rows = _expanded_ranking_rows(sources)

    if retest.get("best_candidate_after_expanded_screening") != RANKING_TOP_CANDIDATE:
        errors.append("2386 best candidate is not the ranking-top reference candidate")
    if retest.get("best_candidate_decision") != CURRENT_BEST_PREVIOUS_DECISION:
        errors.append("2386 best candidate decision is not CONTINUE_OPTIMIZATION")
    if retest.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2386 unexpectedly marked a candidate observation-ready")
    decision_update = _as_mapping(decision.get("decision_update"))
    if decision_update:
        if decision_update.get("best_candidate_after_expanded_screening") not in (
            None,
            RANKING_TOP_CANDIDATE,
        ):
            errors.append("2386 decision update best candidate mismatch")
        if decision_update.get("best_candidate_decision") not in (
            None,
            CURRENT_BEST_PREVIOUS_DECISION,
        ):
            errors.append("2386 decision update best decision mismatch")

    present = {str(row.get("candidate_id")) for row in rows}
    for candidate_id in REQUIRED_FOCUS_CANDIDATES:
        if candidate_id not in present:
            errors.append(f"2386 ranking missing focus candidate {candidate_id}")


def _validate_2365_2366_context(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    ranking_2365 = _as_mapping(sources.get("candidate_ranking_2365"))
    rows_2365 = _as_list_of_mappings(ranking_2365.get("candidate_ranking"))
    top_2365 = next(
        (row for row in rows_2365 if row.get("candidate_id") == RANKING_TOP_CANDIDATE),
        {},
    )
    if not top_2365:
        errors.append("2365 candidate ranking missing ranking-top reference candidate")
    elif top_2365.get("decision") not in ("OWNER_REVIEW_REQUIRED", "CONTINUE_RESEARCH"):
        errors.append("2365 ranking-top candidate has unexpected decision")

    sensitivity = _as_mapping(sources.get("sensitivity_result_2366"))
    if sensitivity.get("top_candidate_from_2365") not in (None, RANKING_TOP_CANDIDATE):
        errors.append("2366 sensitivity top_candidate_from_2365 mismatch")


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
        "source_ready_for_candidate_reclassification": bool(
            sources.get("source_ready_for_candidate_reclassification")
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
    rows = _expanded_ranking_rows(sources)
    matrix_rows = _candidate_threshold_rows(sources)
    preview = _candidate_reclassification_preview(rows, matrix_rows)
    component_review = _component_attribution_review(rows)
    owner_recommendation = _owner_review_recommendation(preview, component_review)
    current_best = _current_best_review(preview, rows, matrix_rows)
    return {
        "calibrated_gate_policy_source": "TRADING-2389",
        "reference_candidate_policy": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
        "candidate_reclassification_ready": True,
        "component_attribution_ready": True,
        "owner_review_recommendation_ready": True,
        "current_best_candidate": RANKING_TOP_CANDIDATE,
        "current_best_candidate_previous_decision": CURRENT_BEST_PREVIOUS_DECISION,
        "current_best_candidate_preview_decision": CURRENT_BEST_PREVIEW_DECISION,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "candidate_reclassification_preview": preview,
        "component_attribution_review": component_review,
        "owner_review_recommendation": owner_recommendation,
        "current_best_candidate_review": current_best,
        "component_value_candidates": list(COMPONENT_VALUE_CANDIDATES),
        "candidate_auto_accept_candidates": [],
        "research_only_observation_approval_candidates": [],
        "explicit_non_approval_list": [
            "candidate_auto_accept",
            "research_only_observation_for_candidate",
            "paper_shadow",
            "event_append",
            "outcome_binding",
            "scheduler",
            "daily_report",
            "production",
            "broker_order",
        ],
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "calibrated_gate_policy_source": "TRADING-2389",
        "reference_candidate_policy": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
        "candidate_reclassification_ready": False,
        "component_attribution_ready": False,
        "owner_review_recommendation_ready": False,
        "current_best_candidate": RANKING_TOP_CANDIDATE,
        "current_best_candidate_previous_decision": None,
        "current_best_candidate_preview_decision": None,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "candidate_reclassification_preview": [],
        "component_attribution_review": [],
        "owner_review_recommendation": {
            "recommendation": "BLOCKED_SOURCE_ARTIFACT_REVIEW_REQUIRED",
            "recommended_next_research_task": None,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "current_best_candidate_review": {},
        "component_value_candidates": [],
        "candidate_auto_accept_candidates": [],
        "research_only_observation_approval_candidates": [],
        "explicit_non_approval_list": [
            "candidate_auto_accept",
            "research_only_observation_for_candidate",
            "paper_shadow",
            "event_append",
            "outcome_binding",
            "scheduler",
            "daily_report",
            "production",
            "broker_order",
        ],
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources),
        "recommended_next_research_task": None,
    }


def _candidate_reclassification_preview(
    ranking_rows: list[dict[str, Any]],
    threshold_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matrix_by_candidate = {
        str(row.get("candidate_id")): row
        for row in threshold_rows
        if row.get("candidate_id")
    }
    preview: list[dict[str, Any]] = []
    for row in ranking_rows:
        candidate_id = str(row.get("candidate_id"))
        matrix_row = matrix_by_candidate.get(candidate_id, {})
        preview_decision = _preview_decision(candidate_id, row, matrix_row)
        overall_decision = (
            CONTINUE_OPTIMIZATION_DECISION
            if preview_decision == COMPONENT_VALUE_DECISION
            else preview_decision
        )
        preview.append(
            {
                "candidate_id": candidate_id,
                "role": FOCUS_CANDIDATE_ROLES.get(candidate_id, "expanded_pool_candidate"),
                "focus_candidate": candidate_id in REQUIRED_FOCUS_CANDIDATES,
                "rank": row.get("rank"),
                "candidate_type": row.get("candidate_type"),
                "signal_family": row.get("signal_family"),
                "previous_decision": row.get("decision"),
                "source_2388_likely_reclassification": matrix_row.get(
                    "likely_reclassification_under_calibrated_gate"
                ),
                "preview_decision": preview_decision,
                "overall_candidate_gate_decision": overall_decision,
                "actual_approval_in_this_task": False,
                "auto_accept_allowed": False,
                "owner_review_allowed": preview_decision
                in (CURRENT_BEST_PREVIEW_DECISION, COMPONENT_VALUE_DECISION),
                "component_value_only": preview_decision == COMPONENT_VALUE_DECISION,
                "reclassification_reason": _reclassification_reason(
                    candidate_id,
                    row,
                    matrix_row,
                    preview_decision,
                ),
                "supporting_metrics": _supporting_metrics(row),
                "failure_metrics": _failure_metrics(row, matrix_row),
            }
        )
    return preview


def _preview_decision(
    candidate_id: str,
    row: Mapping[str, Any],
    matrix_row: Mapping[str, Any],
) -> str:
    if candidate_id == RANKING_TOP_CANDIDATE:
        return CURRENT_BEST_PREVIEW_DECISION
    if candidate_id in COMPONENT_VALUE_CANDIDATES:
        return COMPONENT_VALUE_DECISION
    if _as_float(row.get("dynamic_vs_static_gap")) <= 0:
        return REJECT_FOR_NOW_DECISION
    if row.get("realistic_cost_passed") is False or row.get("conservative_cost_passed") is False:
        return REJECT_FOR_NOW_DECISION
    likely = matrix_row.get("likely_reclassification_under_calibrated_gate")
    if likely in (
        CONTINUE_OPTIMIZATION_DECISION,
        REJECT_FOR_NOW_DECISION,
        DEPRECATED_BY_CALIBRATED_GATE_DECISION,
    ):
        return str(likely)
    return CONTINUE_OPTIMIZATION_DECISION


def _reclassification_reason(
    candidate_id: str,
    row: Mapping[str, Any],
    matrix_row: Mapping[str, Any],
    preview_decision: str,
) -> list[str]:
    if candidate_id == RANKING_TOP_CANDIDATE:
        return [
            "positive dynamic_vs_static_gap",
            "realistic/conservative/harsh cost stress passed",
            "turnover budget passed",
            "2389 policy blocks auto-accept but allows owner review",
            "time/regime slice instability prevents auto-accept",
            "drawdown materiality prevents observation approval in this task",
        ]
    if preview_decision == COMPONENT_VALUE_DECISION:
        return [
            "overall candidate remains below calibrated owner-review threshold",
            "component has reusable evidence from cost/turnover or signal-validity behavior",
            "regime-slice weakness and guarded/ranking gaps block full candidate approval",
            "component-level targeted improvement is required before owner observation decision",
        ]
    if preview_decision == REJECT_FOR_NOW_DECISION:
        return [
            "candidate fails positive dynamic gap or cost-stress requirements",
            "no auto-accept or observation approval is available in TRADING-2390",
        ]
    reasons = list(row.get("decision_reasons", []))[:6]
    blockers = list(matrix_row.get("current_gate_blockers", []))
    return [
        "calibrated gate still requires further optimization",
        *[str(item) for item in reasons],
        *[f"2388_blocker={item}" for item in blockers[:4]],
    ]


def _component_attribution_review(
    ranking_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows_by_candidate = {
        str(row.get("candidate_id")): row for row in ranking_rows if row.get("candidate_id")
    }
    family_specs = [
        {
            "component_name": "turnover_budgeting",
            "source_candidates": ["dynamic_turnover_budgeted_growth_tilt_v1"],
            "component_value_hypothesis": [
                "explicit turnover budget",
                "cost-aware growth tilt",
                "better turnover discipline",
            ],
            "recommended_followup": (
                "reuse turnover budget discipline inside a higher-return candidate "
                "without weakening regime-slice evidence"
            ),
        },
        {
            "component_name": "valid_until_strictness",
            "source_candidates": ["dynamic_valid_until_expiry_strict_v1"],
            "component_value_hypothesis": [
                "stale signal prevention",
                "stricter signal expiry",
                "reduced carry-forward risk",
            ],
            "recommended_followup": (
                "test valid-until strictness as a component overlay rather than a "
                "standalone candidate"
            ),
        },
        {
            "component_name": "growth_tilt_engine",
            "source_candidates": [RANKING_TOP_CANDIDATE],
            "component_value_hypothesis": [
                "return advantage",
                "upside capture",
                "risk-on responsiveness",
            ],
            "recommended_followup": (
                "keep growth tilt as the owner-review reference while repairing "
                "drawdown and slice instability"
            ),
        },
        {
            "component_name": "lower_turnover_guardrail",
            "source_candidates": [
                "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
                "dynamic_regime_overlay_v0_4_lower_turnover",
            ],
            "component_value_hypothesis": [
                "turnover reduction",
                "cost stress resilience",
                "execution discipline",
            ],
            "recommended_followup": (
                "reuse lower-turnover guardrails as constraints, not as a direct "
                "replacement for the ranking-top return engine"
            ),
        },
        {
            "component_name": "guarded_turnover_transfer",
            "source_candidates": ["equal_risk_growth_tilt_guarded_turnover_v1"],
            "component_value_hypothesis": [
                "partial transfer of lower-turnover guardrail to ranking top",
                "reduced fragility relative to original ranking top",
            ],
            "recommended_followup": (
                "compare guarded transfer against the original ranking top in "
                "owner-review materials without treating it as approved observation"
            ),
        },
        {
            "component_name": "risk_cap_interaction",
            "source_candidates": ["dynamic_risk_cap_adaptive_v1"],
            "component_value_hypothesis": [
                "risk-cap interaction can reduce downside exposure",
                "risk-cap logic may be reusable as a veto-like component",
            ],
            "recommended_followup": (
                "retain only as component-level research until return/ranking gaps improve"
            ),
        },
        {
            "component_name": "regime_transition_reentry",
            "source_candidates": [
                "dynamic_regime_reentry_accelerated_v1",
                "dynamic_regime_recovery_confirmation_v1",
            ],
            "component_value_hypothesis": [
                "regime transition handling may reduce late re-risk behavior",
                "reentry confirmation may improve recovery participation",
            ],
            "recommended_followup": (
                "use as diagnostic input for component-level targeted improvement"
            ),
        },
    ]
    review: list[dict[str, Any]] = []
    for spec in family_specs:
        source_candidates = list(spec["source_candidates"])
        candidate_rows = [
            rows_by_candidate[candidate_id]
            for candidate_id in source_candidates
            if candidate_id in rows_by_candidate
        ]
        review.append(
            {
                "component_name": spec["component_name"],
                "source_candidates": source_candidates,
                "component_value_hypothesis": list(spec["component_value_hypothesis"]),
                "supporting_metrics": [
                    _supporting_metrics(row) for row in candidate_rows
                ],
                "failure_metrics": [_failure_metrics(row, {}) for row in candidate_rows],
                "reusable_in_future_candidate": _component_reusable(candidate_rows),
                "recommended_followup": spec["recommended_followup"],
            }
        )
    return review


def _owner_review_recommendation(
    preview: list[dict[str, Any]],
    component_review: list[dict[str, Any]],
) -> dict[str, Any]:
    current_best = next(
        row for row in preview if row["candidate_id"] == RANKING_TOP_CANDIDATE
    )
    return {
        "owner_review_recommendation_ready": True,
        "recommendation": (
            "PROCEED_TO_2391_OWNER_REVIEW_DECISION_WITH_NO_OBSERVATION_APPROVAL_IN_2390"
        ),
        "enter_owner_review_decision": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "current_best_candidate": RANKING_TOP_CANDIDATE,
        "current_best_candidate_preview_decision": current_best["preview_decision"],
        "component_value_candidates": list(COMPONENT_VALUE_CANDIDATES),
        "component_family_count": len(component_review),
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "decision_boundary": (
            "TRADING-2390 is preview and attribution only; any observation decision "
            "must be recorded in TRADING-2391."
        ),
    }


def _current_best_review(
    preview: list[dict[str, Any]],
    ranking_rows: list[dict[str, Any]],
    threshold_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    row = next(
        item for item in ranking_rows if item.get("candidate_id") == RANKING_TOP_CANDIDATE
    )
    matrix = next(
        (
            item
            for item in threshold_rows
            if item.get("candidate_id") == RANKING_TOP_CANDIDATE
        ),
        {},
    )
    preview_row = next(
        item for item in preview if item.get("candidate_id") == RANKING_TOP_CANDIDATE
    )
    return {
        "candidate_id": RANKING_TOP_CANDIDATE,
        "previous_decision": row.get("decision"),
        "preview_decision": preview_row.get("preview_decision"),
        "source_2388_likely_reclassification": matrix.get(
            "likely_reclassification_under_calibrated_gate"
        ),
        "supporting_metrics": _supporting_metrics(row),
        "failure_metrics": _failure_metrics(row, matrix),
        "auto_accept_allowed": False,
        "research_only_observation_approved": False,
        "owner_review_allowed": True,
        "rationale": [
            "positive dynamic_vs_static_gap and cost-stress survival justify owner review",
            "slice instability and drawdown materiality block automatic acceptance",
            "TRADING-2390 cannot approve observation; TRADING-2391 must record owner decision",
        ],
    }


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    retest = _as_mapping(sources.get("expanded_candidate_retest"))
    owner = _as_mapping(sources.get("owner_review_decision"))
    methodology = _as_mapping(sources.get("threshold_methodology_review"))
    ranking_2365 = _as_mapping(sources.get("candidate_ranking_2365"))
    sensitivity = _as_mapping(sources.get("sensitivity_result_2366"))
    return {
        "trading_2365": {
            "status": ranking_2365.get("status"),
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
            "candidate_ready_for_research_only_observation": retest.get(
                "candidate_ready_for_research_only_observation"
            ),
        },
        "trading_2388": {
            "status": methodology.get("status"),
            "reference_candidate_policy_recommendation": methodology.get(
                "reference_candidate_policy_recommendation"
            ),
            "research_only_vs_paper_shadow_gate_separated": methodology.get(
                "research_only_vs_paper_shadow_gate_separated"
            ),
        },
        "trading_2389": {
            "status": owner.get("status"),
            "owner_decision": owner.get("owner_decision"),
            "reference_candidate_policy_adopted": owner.get(
                "reference_candidate_policy_adopted"
            ),
            "candidate_auto_accept_approved": owner.get(
                "candidate_auto_accept_approved"
            ),
            "current_best_candidate_observation_approved": owner.get(
                "current_best_candidate_observation_approved"
            ),
        },
    }


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "RECLASSIFICATION_PREVIEW_AND_COMPONENT_ATTRIBUTION_ONLY",
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


def _supporting_metrics(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "dynamic_vs_static_gap": row.get("dynamic_vs_static_gap"),
        "cost_adjusted_dynamic_vs_static_gap": row.get(
            "cost_adjusted_dynamic_vs_static_gap"
        ),
        "realistic_cost_passed": row.get("realistic_cost_passed"),
        "conservative_cost_passed": row.get("conservative_cost_passed"),
        "harsh_cost_passed": row.get("harsh_cost_passed"),
        "turnover_budget_passed": row.get("turnover_budget_passed"),
        "time_slice_pass_rate": row.get("time_slice_pass_rate"),
        "regime_slice_pass_rate": row.get("regime_slice_pass_rate"),
        "return_advantage_retained": row.get("return_advantage_retained"),
        "candidate_vs_ranking_top_gap": row.get("candidate_vs_ranking_top_gap"),
        "candidate_vs_guarded_ranking_top_gap": row.get(
            "candidate_vs_guarded_ranking_top_gap"
        ),
        "candidate_vs_lower_turnover_gap": row.get("candidate_vs_lower_turnover_gap"),
        "turnover": row.get("turnover"),
        "max_monthly_turnover": row.get("max_monthly_turnover"),
        "no_stale_signal_carry_forward": row.get("no_stale_signal_carry_forward"),
        "valid_until_window_preserved": row.get("valid_until_window_preserved"),
    }


def _failure_metrics(
    row: Mapping[str, Any],
    matrix_row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "drawdown_gap_vs_static": row.get("drawdown_gap_vs_static"),
        "drawdown_not_materially_worse": matrix_row.get(
            "drawdown_not_materially_worse",
            _drawdown_not_materially_worse(row),
        ),
        "time_slice_pass_rate": row.get("time_slice_pass_rate"),
        "regime_slice_pass_rate": row.get("regime_slice_pass_rate"),
        "candidate_vs_ranking_top_gap": row.get("candidate_vs_ranking_top_gap"),
        "candidate_vs_guarded_ranking_top_gap": row.get(
            "candidate_vs_guarded_ranking_top_gap"
        ),
        "return_advantage_retained": row.get("return_advantage_retained"),
        "current_gate_blockers": list(matrix_row.get("current_gate_blockers", [])),
    }


def _component_reusable(candidate_rows: list[dict[str, Any]]) -> bool:
    return any(
        row.get("turnover_budget_passed") is True
        or row.get("no_stale_signal_carry_forward") is True
        or _as_float(row.get("dynamic_vs_static_gap")) > 0
        for row in candidate_rows
    )


def _drawdown_not_materially_worse(row: Mapping[str, Any]) -> bool:
    value = row.get("drawdown_gap_vs_static")
    return isinstance(value, int | float) and float(value) <= 0


def _expanded_ranking_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    ranking_doc = _as_mapping(sources.get("expanded_candidate_ranking"))
    rows = _as_list_of_mappings(ranking_doc.get("expanded_candidate_ranking"))
    if rows:
        return rows
    retest = _as_mapping(sources.get("expanded_candidate_retest"))
    return _as_list_of_mappings(retest.get("expanded_candidate_ranking"))


def _candidate_threshold_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    matrix_doc = _as_mapping(sources.get("candidate_threshold_outcome_matrix"))
    rows = _as_list_of_mappings(matrix_doc.get("candidate_threshold_outcome_matrix"))
    if rows:
        return rows
    methodology = _as_mapping(sources.get("threshold_methodology_review"))
    return _as_list_of_mappings(methodology.get("candidate_threshold_outcome_matrix"))


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "json_path": str(output_root / "reclassification_result.json"),
        "candidate_reclassification_preview_json": str(
            output_root / "candidate_reclassification_preview.json"
        ),
        "component_attribution_review_json": str(
            output_root / "component_attribution_review.json"
        ),
        "owner_review_recommendation_json": str(
            output_root / "owner_review_recommendation.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_calibrated_gate_candidate_reclassification.md"
        ),
        "candidate_reclassification_preview_markdown": str(
            docs_root
            / "dynamic_strategy_calibrated_gate_candidate_reclassification_preview.md"
        ),
        "component_attribution_review_markdown": str(
            docs_root / "dynamic_strategy_calibrated_gate_component_attribution_review.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2391_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["candidate_reclassification_preview_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_calibrated_gate_candidate_reclassification_preview",
            "schema_version": (
                "dynamic_strategy_calibrated_gate_candidate_reclassification_preview.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "candidate_reclassification_preview": payload.get(
                "candidate_reclassification_preview", []
            ),
            "current_best_candidate": payload.get("current_best_candidate"),
            "current_best_candidate_preview_decision": payload.get(
                "current_best_candidate_preview_decision"
            ),
            "candidate_auto_accept_approved": False,
            "research_only_observation_approved": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["component_attribution_review_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_calibrated_gate_component_attribution_review",
            "schema_version": "dynamic_strategy_calibrated_gate_component_attribution_review.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "component_attribution_review": payload.get("component_attribution_review", []),
            "component_value_candidates": payload.get("component_value_candidates", []),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["owner_review_recommendation_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_calibrated_gate_owner_review_recommendation",
            "schema_version": "dynamic_strategy_calibrated_gate_owner_review_recommendation.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "owner_review_recommendation": payload.get("owner_review_recommendation", {}),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "candidate_auto_accept_approved": False,
            "research_only_observation_approved": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["candidate_reclassification_preview_markdown"]),
        _candidate_preview_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["component_attribution_review_markdown"]),
        _component_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy calibrated gate candidate reclassification",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as_of：`{payload.get('as_of')}`",
            f"- source policy：`{payload.get('calibrated_gate_policy_source')}`",
            f"- reference policy：`{payload.get('reference_candidate_policy')}`",
            f"- current best：`{payload.get('current_best_candidate')}`",
            f"- previous decision：`{payload.get('current_best_candidate_previous_decision')}`",
            f"- preview decision：`{payload.get('current_best_candidate_preview_decision')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Executive summary",
            "",
            (
                "TRADING-2390 applies the owner-adopted calibrated research-only gate "
                "from TRADING-2389 to the TRADING-2386 expanded candidate pool. The "
                "current best candidate is reclassified as owner-review-required in "
                "preview, but no candidate is auto-accepted or approved for observation."
            ),
            "",
            "## Source findings",
            "",
            "```json",
            _json_block(payload.get("source_findings")),
            "```",
            "",
            "## Calibrated gate policy recap",
            "",
            "- Research-only observation gate is artifact-only and side-effect-free.",
            "- Reference candidate policy is `BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`.",
            "- Paper-shadow, production, broker, event append and outcome binding remain disabled.",
            "",
            "## Candidate reclassification preview",
            "",
            _preview_table(payload.get("candidate_reclassification_preview", [])),
            "",
            "## Current best candidate review",
            "",
            "```json",
            _json_block(payload.get("current_best_candidate_review")),
            "```",
            "",
            "## Component attribution review",
            "",
            _component_table(payload.get("component_attribution_review", [])),
            "",
            "## Owner review recommendation",
            "",
            "```json",
            _json_block(payload.get("owner_review_recommendation")),
            "```",
            "",
            "## Explicit non-approval list",
            "",
            "\n".join(f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])),
            "",
            "## Guardrail summary",
            "",
            "```json",
            _json_block(payload.get("guardrail_summary")),
            "```",
        ]
    )


def _candidate_preview_markdown(payload: Mapping[str, Any]) -> str:
    current_best_preview = payload.get("current_best_candidate_preview_decision")
    return "\n".join(
        [
            "# Dynamic strategy calibrated gate candidate reclassification preview",
            "",
            f"- status：`{payload.get('status')}`",
            f"- current best preview decision：`{current_best_preview}`",
            "- candidate auto-accept approved：`False`",
            "- research-only observation approved：`False`",
            "",
            _preview_table(payload.get("candidate_reclassification_preview", [])),
        ]
    )


def _component_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy calibrated gate component attribution review",
            "",
            f"- status：`{payload.get('status')}`",
            f"- component value candidates：`{payload.get('component_value_candidates')}`",
            "",
            _component_table(payload.get("component_attribution_review", [])),
            "",
            "```json",
            _json_block(payload.get("component_attribution_review")),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy TRADING-2391 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- recommended next route：`{payload.get('recommended_next_research_task')}`",
            "- next step：owner review decision and observation decision record",
            "- observation approved in TRADING-2390：`False`",
            "- paper-shadow enabled：`False`",
            "- production enabled：`False`",
            "- broker action enabled：`False`",
            "",
            "TRADING-2391 must record whether the project owner approves any "
            "research-only observation. TRADING-2390 only provides preview and "
            "component attribution evidence.",
        ]
    )


def _preview_table(rows: Any) -> str:
    items = _as_list_of_mappings(rows)
    lines = [
        "|Candidate|Role|Previous|Preview|Component value|Auto accept|Observation approved|",
        "|---|---|---|---|---|---|---|",
    ]
    for row in items:
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{row.get('candidate_id')}`",
                    str(row.get("role")),
                    f"`{row.get('previous_decision')}`",
                    f"`{row.get('preview_decision')}`",
                    f"`{row.get('component_value_only')}`",
                    f"`{row.get('auto_accept_allowed')}`",
                    "`False`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _component_table(rows: Any) -> str:
    items = _as_list_of_mappings(rows)
    lines = [
        "|Component|Source candidates|Reusable|Recommended follow-up|",
        "|---|---|---|---|",
    ]
    for row in items:
        candidates = ", ".join(f"`{item}`" for item in row.get("source_candidates", []))
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{row.get('component_name')}`",
                    candidates,
                    f"`{row.get('reusable_in_future_candidate')}`",
                    str(row.get("recommended_followup")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _json_block(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _source_document_names() -> tuple[str, ...]:
    return (
        "candidate_ranking_2365",
        "sensitivity_result_2366",
        "expanded_candidate_retest",
        "expanded_candidate_ranking",
        "expanded_decision_update",
        "threshold_methodology_review",
        "candidate_threshold_outcome_matrix",
        "recommended_gate_policy_proposal",
        "owner_review_decision",
        "calibrated_gate_adoption_record",
        "non_approval_record",
        "next_reclassification_route",
    )


def _load_json_document(path: Path) -> Any:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _as_float(value: Any) -> float:
    if isinstance(value, int | float):
        return float(value)
    return 0.0
