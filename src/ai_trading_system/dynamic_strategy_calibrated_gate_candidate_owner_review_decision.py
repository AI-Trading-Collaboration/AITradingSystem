from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification import (
    COMPONENT_VALUE_CANDIDATES,
    CURRENT_BEST_PREVIEW_DECISION,
    CURRENT_BEST_PREVIOUS_DECISION,
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification import (
    NEXT_ROUTE as SOURCE_2390_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification import (
    READY_STATUS as SOURCE_2390_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision import (
    OWNER_DECISION as SOURCE_2389_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision import (
    READY_STATUS as SOURCE_2389_READY_STATUS,
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

TASK_ID = "TRADING-2391"
TASK_REGISTER_ID = (
    "TRADING-2391_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_"
    "AND_OBSERVATION_DECISION"
)
REPORT_TYPE = "dynamic_strategy_calibrated_gate_candidate_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_calibrated_gate_candidate_owner_review_and_observation_"
    "decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_"
    "OBSERVATION_DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_"
    "OBSERVATION_DECISION_BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = (
    "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_"
    "COMPONENT_ATTRIBUTION"
)
DEFAULT_OWNER_DECISION_OPTION = "OPTION_B_KEEP_OWNER_REVIEW_REQUIRED_NO_OBSERVATION"
NEXT_ROUTE = "TRADING-2392_Dynamic_Strategy_Component_Attribution_And_Gate_Evidence_Plan"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2386",
    "TRADING-2388",
    "TRADING-2389",
    "TRADING-2390",
)
KNOWN_RISK_REASONS: tuple[str, ...] = (
    "time_slice_instability",
    "regime_slice_instability",
    "drawdown_materiality_requires_owner_judgment",
    "reference_candidate_auto_accept_blocked",
)
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
)

DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "threshold_methodology_review_result.json"
)
DEFAULT_SOURCE_2389_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "reclassification_result.json"
)
DEFAULT_SOURCE_2390_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "candidate_reclassification_preview.json"
)
DEFAULT_SOURCE_2390_COMPONENT_ATTRIBUTION_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "component_attribution_review.json"
)
DEFAULT_SOURCE_2390_OWNER_REVIEW_RECOMMENDATION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "owner_review_recommendation.json"
)


def run_dynamic_strategy_calibrated_gate_candidate_owner_review_decision(
    *,
    source_expanded_candidate_retest_2386_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_threshold_methodology_review_2388_path: Path = (
        DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH
    ),
    source_owner_review_decision_2389_path: Path = (
        DEFAULT_SOURCE_2389_OWNER_REVIEW_DECISION_PATH
    ),
    source_reclassification_result_2390_path: Path = (
        DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH
    ),
    source_candidate_reclassification_preview_2390_path: Path = (
        DEFAULT_SOURCE_2390_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH
    ),
    source_component_attribution_review_2390_path: Path = (
        DEFAULT_SOURCE_2390_COMPONENT_ATTRIBUTION_REVIEW_PATH
    ),
    source_owner_review_recommendation_2390_path: Path = (
        DEFAULT_SOURCE_2390_OWNER_REVIEW_RECOMMENDATION_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_expanded_candidate_retest_2386_path=(
            source_expanded_candidate_retest_2386_path
        ),
        source_threshold_methodology_review_2388_path=(
            source_threshold_methodology_review_2388_path
        ),
        source_owner_review_decision_2389_path=source_owner_review_decision_2389_path,
        source_reclassification_result_2390_path=(
            source_reclassification_result_2390_path
        ),
        source_candidate_reclassification_preview_2390_path=(
            source_candidate_reclassification_preview_2390_path
        ),
        source_component_attribution_review_2390_path=(
            source_component_attribution_review_2390_path
        ),
        source_owner_review_recommendation_2390_path=(
            source_owner_review_recommendation_2390_path
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
    sources["source_ready_for_owner_review_decision"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "expanded_candidate_retest_2386": SOURCE_2386_READY_STATUS,
        "threshold_methodology_review_2388": SOURCE_2388_READY_STATUS,
        "owner_review_decision_2389": SOURCE_2389_READY_STATUS,
        "reclassification_result_2390": SOURCE_2390_READY_STATUS,
        "candidate_reclassification_preview_2390": SOURCE_2390_READY_STATUS,
        "component_attribution_review_2390": SOURCE_2390_READY_STATUS,
        "owner_review_recommendation_2390": SOURCE_2390_READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )

    _validate_2386_candidate_context(sources, errors)
    _validate_2388_gate_policy(sources, errors)
    _validate_2389_owner_decision(sources, errors)
    _validate_2390_reclassification(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2386_candidate_context(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("expanded_candidate_retest_2386"))
    if retest.get("best_candidate_after_expanded_screening") != RANKING_TOP_CANDIDATE:
        errors.append("2386 best candidate is not the calibrated owner-review candidate")
    if retest.get("best_candidate_decision") != CURRENT_BEST_PREVIOUS_DECISION:
        errors.append("2386 best candidate decision is not CONTINUE_OPTIMIZATION")
    if retest.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2386 unexpectedly marked candidate observation-ready")


def _validate_2388_gate_policy(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    methodology = _as_mapping(sources.get("threshold_methodology_review_2388"))
    if methodology.get("threshold_methodology_review_ready") is not True:
        errors.append("2388 threshold_methodology_review_ready is not true")
    if methodology.get("research_only_vs_paper_shadow_gate_separated") is not True:
        errors.append("2388 did not separate research-only and paper-shadow gates")
    if methodology.get("reference_candidate_policy_recommendation") != (
        REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2388 reference policy recommendation mismatch")


def _validate_2389_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2389"))
    if owner.get("owner_decision") != SOURCE_2389_OWNER_DECISION:
        errors.append("2389 owner decision did not adopt calibrated gate policy")
    if owner.get("reference_candidate_policy_adopted") != (
        REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2389 reference candidate policy was not adopted")
    for field in (
        "threshold_methodology_adopted",
        "research_only_vs_paper_shadow_gate_separated",
        "calibrated_reclassification_preview_approved",
        "component_attribution_review_required",
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


def _validate_2390_reclassification(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    reclassification = _as_mapping(sources.get("reclassification_result_2390"))
    preview_doc = _as_mapping(sources.get("candidate_reclassification_preview_2390"))
    component_doc = _as_mapping(sources.get("component_attribution_review_2390"))
    recommendation_doc = _as_mapping(sources.get("owner_review_recommendation_2390"))
    recommendation = _as_mapping(recommendation_doc.get("owner_review_recommendation"))

    if reclassification.get("recommended_next_research_task") != (
        SOURCE_2390_EXPECTED_ROUTE
    ):
        errors.append("2390 reclassification does not route to TRADING-2391")
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
    if reclassification.get("reference_candidate_policy") != (
        REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2390 reference candidate policy mismatch")
    if reclassification.get("candidate_auto_accept_approved") is True:
        errors.append("2390 unexpectedly approved candidate auto-accept")
    if reclassification.get("research_only_observation_approved") is True:
        errors.append("2390 unexpectedly approved research-only observation")

    preview_rows = _preview_rows(sources)
    current_best_rows = [
        row for row in preview_rows if row.get("candidate_id") == RANKING_TOP_CANDIDATE
    ]
    if not current_best_rows:
        errors.append("2390 preview missing current best candidate row")
    else:
        current_best = current_best_rows[0]
        if current_best.get("previous_decision") != CURRENT_BEST_PREVIOUS_DECISION:
            errors.append("2390 preview current best previous decision mismatch")
        if current_best.get("preview_decision") != CURRENT_BEST_PREVIEW_DECISION:
            errors.append("2390 preview current best calibrated decision mismatch")
        if current_best.get("actual_approval_in_this_task") is True:
            errors.append("2390 preview unexpectedly approved current best")

    component_candidates = set(
        str(item) for item in component_doc.get("component_value_candidates", [])
    )
    if not set(COMPONENT_VALUE_CANDIDATES).issubset(component_candidates):
        errors.append("2390 component attribution missing required component candidates")
    if recommendation.get("enter_owner_review_decision") is not True:
        errors.append("2390 owner review recommendation did not enter owner review")
    if recommendation.get("research_only_observation_approved") is True:
        errors.append("2390 owner review recommendation unexpectedly approved observation")
    if preview_doc.get("current_best_candidate_preview_decision") != (
        CURRENT_BEST_PREVIEW_DECISION
    ):
        errors.append("2390 preview artifact current best decision mismatch")


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
    candidate_record = _candidate_owner_review_record(sources)
    non_approval_record = _observation_non_approval_record(candidate_record)
    next_route = _next_route_record(candidate_record)
    return {
        "current_best_candidate": RANKING_TOP_CANDIDATE,
        "previous_decision": CURRENT_BEST_PREVIOUS_DECISION,
        "calibrated_preview_decision": CURRENT_BEST_PREVIEW_DECISION,
        "current_best_candidate_previous_decision": CURRENT_BEST_PREVIOUS_DECISION,
        "current_best_candidate_preview_decision": CURRENT_BEST_PREVIEW_DECISION,
        "owner_review_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "owner_decision_option": DEFAULT_OWNER_DECISION_OPTION,
        "owner_review_required_retained": True,
        "research_only_observation_approved": False,
        "candidate_auto_accept_approved": False,
        "component_attribution_continue_recommended": True,
        "component_value_candidates": list(COMPONENT_VALUE_CANDIDATES),
        "known_risk_reasons": list(KNOWN_RISK_REASONS),
        "decision_inputs": _decision_inputs(sources),
        "candidate_owner_review_record": candidate_record,
        "observation_non_approval_record": non_approval_record,
        "owner_decision_options": _owner_decision_options(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources),
        "next_route": next_route,
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "current_best_candidate": RANKING_TOP_CANDIDATE,
        "previous_decision": None,
        "calibrated_preview_decision": None,
        "current_best_candidate_previous_decision": None,
        "current_best_candidate_preview_decision": None,
        "owner_review_decision_recorded": False,
        "owner_decision": None,
        "owner_decision_option": None,
        "owner_review_required_retained": False,
        "research_only_observation_approved": False,
        "candidate_auto_accept_approved": False,
        "component_attribution_continue_recommended": False,
        "component_value_candidates": [],
        "known_risk_reasons": list(KNOWN_RISK_REASONS),
        "decision_inputs": {},
        "candidate_owner_review_record": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "observation_non_approval_record": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "owner_decision_options": _owner_decision_options(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "guardrail_summary": _guardrail_summary(),
        "source_findings": _source_findings(sources),
        "next_route": {},
        "recommended_next_research_task": None,
    }


def _decision_inputs(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "current_best_candidate": {
            "candidate_id": RANKING_TOP_CANDIDATE,
            "previous_decision": CURRENT_BEST_PREVIOUS_DECISION,
            "calibrated_preview_decision": CURRENT_BEST_PREVIEW_DECISION,
        },
        "calibrated_gate_policy": {
            "source_task": "TRADING-2389",
            "reference_candidate_policy": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
            "research_only_vs_paper_shadow_gate_separated": True,
            "auto_accept_approved": False,
        },
        "source_2390_reclassification": {
            "status": _as_mapping(sources.get("reclassification_result_2390")).get(
                "status"
            ),
            "recommended_next_research_task": _as_mapping(
                sources.get("reclassification_result_2390")
            ).get("recommended_next_research_task"),
        },
        "component_value_candidates": list(COMPONENT_VALUE_CANDIDATES),
        "known_risk_reasons": list(KNOWN_RISK_REASONS),
    }


def _candidate_owner_review_record(sources: Mapping[str, Any]) -> dict[str, Any]:
    current_best_preview = next(
        (
            row
            for row in _preview_rows(sources)
            if row.get("candidate_id") == RANKING_TOP_CANDIDATE
        ),
        {},
    )
    return {
        "record_ready": True,
        "candidate_id": RANKING_TOP_CANDIDATE,
        "previous_decision": CURRENT_BEST_PREVIOUS_DECISION,
        "calibrated_preview_decision": CURRENT_BEST_PREVIEW_DECISION,
        "owner_review_required_retained": True,
        "owner_review_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "owner_decision_option": DEFAULT_OWNER_DECISION_OPTION,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "paper_shadow_approved": False,
        "component_attribution_continue_recommended": True,
        "component_value_candidates": list(COMPONENT_VALUE_CANDIDATES),
        "supporting_metrics": dict(
            _as_mapping(current_best_preview.get("supporting_metrics"))
        ),
        "failure_metrics": dict(_as_mapping(current_best_preview.get("failure_metrics"))),
        "known_risk_reasons": list(KNOWN_RISK_REASONS),
        "decision_rationale": [
            "2390 only produced a calibrated reclassification preview",
            "OWNER_REVIEW_REQUIRED does not equal observation approval",
            "time/regime slice instability and drawdown materiality still require owner judgment",
            "no explicit owner approval for research-only observation is present",
            "component-level follow-up remains useful before any observation protocol update",
        ],
    }


def _observation_non_approval_record(
    candidate_record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "record_ready": True,
        "candidate_id": candidate_record.get("candidate_id"),
        "owner_decision": OWNER_DECISION,
        "research_only_observation_approved": False,
        "candidate_auto_accept_approved": False,
        "owner_review_required_retained": True,
        "component_attribution_continue_recommended": True,
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
            "owner approval for observation was not explicitly granted",
            "calibrated preview still carries time/regime instability",
            "drawdown materiality prevents automatic acceptance",
            "paper-shadow and execution gates remain separate and closed",
        ],
    }


def _next_route_record(candidate_record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": (
            "candidate remains owner-review-required without observation approval; "
            "component attribution and gate evidence must continue first"
        ),
        "current_best_candidate": candidate_record.get("candidate_id"),
        "owner_decision": OWNER_DECISION,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }


def _owner_decision_options() -> list[dict[str, Any]]:
    return [
        {
            "option": "OPTION_A_APPROVE_RESEARCH_ONLY_OBSERVATION_WITH_NO_EXECUTION",
            "selected": False,
            "meaning": (
                "allow research-only observation while keeping paper-shadow, event, "
                "outcome, scheduler and broker paths disabled"
            ),
        },
        {
            "option": DEFAULT_OWNER_DECISION_OPTION,
            "selected": True,
            "meaning": "do not approve observation, but retain OWNER_REVIEW_REQUIRED",
        },
        {
            "option": "OPTION_C_CONTINUE_COMPONENT_ATTRIBUTION",
            "selected": False,
            "meaning": "study component value before observing the whole candidate",
        },
        {
            "option": "OPTION_D_REQUIRE_STATISTICAL_THRESHOLD_CALIBRATION_FIRST",
            "selected": False,
            "meaning": "build threshold meta-dataset before deciding the candidate",
        },
        {
            "option": "OPTION_E_REJECT_FOR_NOW",
            "selected": False,
            "meaning": "stop investing research effort in the calibrated candidate",
        },
    ]


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    retest = _as_mapping(sources.get("expanded_candidate_retest_2386"))
    methodology = _as_mapping(sources.get("threshold_methodology_review_2388"))
    owner = _as_mapping(sources.get("owner_review_decision_2389"))
    reclassification = _as_mapping(sources.get("reclassification_result_2390"))
    recommendation_doc = _as_mapping(sources.get("owner_review_recommendation_2390"))
    recommendation = _as_mapping(recommendation_doc.get("owner_review_recommendation"))
    return {
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
            "current_best_candidate_observation_approved": owner.get(
                "current_best_candidate_observation_approved"
            ),
            "candidate_auto_accept_approved": owner.get(
                "candidate_auto_accept_approved"
            ),
        },
        "trading_2390": {
            "status": reclassification.get("status"),
            "current_best_candidate": reclassification.get("current_best_candidate"),
            "previous_decision": reclassification.get(
                "current_best_candidate_previous_decision"
            ),
            "calibrated_preview_decision": reclassification.get(
                "current_best_candidate_preview_decision"
            ),
            "component_value_candidates": reclassification.get(
                "component_value_candidates"
            ),
            "owner_review_recommendation": recommendation.get("recommendation"),
            "research_only_observation_approved": reclassification.get(
                "research_only_observation_approved"
            ),
        },
    }


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "OWNER_REVIEW_DECISION_RECORD_ONLY",
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "owner_review_required_retained": True,
        "component_attribution_continue_recommended": True,
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
        "candidate_owner_review_record_json": str(
            output_root / "candidate_owner_review_record.json"
        ),
        "observation_non_approval_record_json": str(
            output_root / "observation_non_approval_record.json"
        ),
        "next_route_json": str(output_root / "next_route.json"),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_calibrated_gate_candidate_owner_review_decision.md"
        ),
        "candidate_owner_review_markdown": str(
            docs_root / "dynamic_strategy_candidate_owner_review_record.md"
        ),
        "observation_non_approval_markdown": str(
            docs_root / "dynamic_strategy_observation_non_approval_record.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2392_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["candidate_owner_review_record_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_candidate_owner_review_record",
            "schema_version": "dynamic_strategy_candidate_owner_review_record.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "candidate_owner_review_record": payload.get(
                "candidate_owner_review_record", {}
            ),
            "current_best_candidate": payload.get("current_best_candidate"),
            "owner_decision": payload.get("owner_decision"),
            "research_only_observation_approved": False,
            "candidate_auto_accept_approved": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["observation_non_approval_record_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_observation_non_approval_record",
            "schema_version": "dynamic_strategy_observation_non_approval_record.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "observation_non_approval_record": payload.get(
                "observation_non_approval_record", {}
            ),
            "research_only_observation_approved": False,
            "candidate_auto_accept_approved": False,
            "paper_shadow_enabled": False,
            "event_append_enabled": False,
            "outcome_binding_enabled": False,
            "scheduler_enabled": False,
            "production_enabled": False,
            "broker_action_enabled": False,
            "daily_report_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["next_route_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_2392_route",
            "schema_version": "dynamic_strategy_2392_route.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "next_route": payload.get("next_route", {}),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["candidate_owner_review_markdown"]),
        _candidate_owner_review_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["observation_non_approval_markdown"]),
        _non_approval_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy calibrated gate candidate owner review decision",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as_of：`{payload.get('as_of')}`",
            f"- current best：`{payload.get('current_best_candidate')}`",
            f"- previous decision：`{payload.get('previous_decision')}`",
            f"- calibrated preview decision：`{payload.get('calibrated_preview_decision')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Executive summary",
            "",
            (
                "TRADING-2391 records the owner-review decision for the calibrated "
                "gate candidate. The candidate remains `OWNER_REVIEW_REQUIRED`, but "
                "research-only observation is not approved because 2390 was only a "
                "preview and no explicit owner approval was provided."
            ),
            "",
            "## Source findings from TRADING-2390",
            "",
            "```json",
            _json_block(payload.get("source_findings")),
            "```",
            "",
            "## Calibrated gate policy recap",
            "",
            "- Reference policy：`BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`.",
            "- `OWNER_REVIEW_REQUIRED` is a manual decision layer, not observation approval.",
            "- Research-only observation and paper-shadow gates remain separated.",
            "",
            "## Current best candidate owner review",
            "",
            "```json",
            _json_block(payload.get("candidate_owner_review_record")),
            "```",
            "",
            "## Owner decision",
            "",
            f"`{payload.get('owner_decision')}`",
            "",
            "## Observation approval / non-approval record",
            "",
            "```json",
            _json_block(payload.get("observation_non_approval_record")),
            "```",
            "",
            "## Component-value follow-up",
            "",
            "\n".join(
                f"- `{item}`" for item in payload.get("component_value_candidates", [])
            ),
            "",
            "## Explicit non-approval list",
            "",
            "\n".join(
                f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])
            ),
            "",
            "## Guardrail summary",
            "",
            "```json",
            _json_block(payload.get("guardrail_summary")),
            "```",
            "",
            "## Recommended next route",
            "",
            "```json",
            _json_block(payload.get("next_route")),
            "```",
        ]
    )


def _candidate_owner_review_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy candidate owner review record",
            "",
            f"- status：`{payload.get('status')}`",
            f"- current best：`{payload.get('current_best_candidate')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            "- research-only observation approved：`False`",
            "- candidate auto-accept approved：`False`",
            "- owner-review-required retained：`True`",
            "",
            "```json",
            _json_block(payload.get("candidate_owner_review_record")),
            "```",
        ]
    )


def _non_approval_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy observation non-approval record",
            "",
            f"- status：`{payload.get('status')}`",
            "- research-only observation approved：`False`",
            "- paper-shadow enabled：`False`",
            "- event append enabled：`False`",
            "- outcome binding enabled：`False`",
            "- scheduler enabled：`False`",
            "- production enabled：`False`",
            "- broker action enabled：`False`",
            "- daily report generated：`False`",
            "",
            "```json",
            _json_block(payload.get("observation_non_approval_record")),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy TRADING-2392 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- recommended next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- route reason：component attribution and gate evidence must continue "
                "before any observation protocol update"
            ),
            "- observation approved in TRADING-2391：`False`",
            "- paper-shadow enabled：`False`",
            "- production enabled：`False`",
            "- broker action enabled：`False`",
            "",
            (
                "TRADING-2392 should continue component attribution and gate evidence "
                "work. It must not treat this decision as observation, paper-shadow, "
                "production or broker approval."
            ),
        ]
    )


def _preview_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    preview_doc = _as_mapping(sources.get("candidate_reclassification_preview_2390"))
    rows = _as_list_of_mappings(preview_doc.get("candidate_reclassification_preview"))
    if rows:
        return rows
    reclassification = _as_mapping(sources.get("reclassification_result_2390"))
    return _as_list_of_mappings(reclassification.get("candidate_reclassification_preview"))


def _source_document_names() -> tuple[str, ...]:
    return (
        "expanded_candidate_retest_2386",
        "threshold_methodology_review_2388",
        "owner_review_decision_2389",
        "reclassification_result_2390",
        "candidate_reclassification_preview_2390",
        "component_attribution_review_2390",
        "owner_review_recommendation_2390",
    )


def _json_block(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


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
