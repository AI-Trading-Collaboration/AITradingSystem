from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_component_recombination_candidate_retest as m2396
import ai_trading_system.dynamic_strategy_recombination_candidate_gate_evidence_plan as m2398
import ai_trading_system.dynamic_strategy_recombination_candidate_owner_review_decision as m2397
import ai_trading_system.dynamic_strategy_targeted_gate_evidence_owner_review_decision as m2400
from ai_trading_system import (
    dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest as m2399,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import json_block as _json_block
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_flag as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2401"
TASK_REGISTER_ID = (
    "TRADING-2401_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_AND_DATA_SIGNAL_"
    "QUALITY_DECISION"
)
REPORT_TYPE = "dynamic_strategy_recombination_line_plateau_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_recombination_line_plateau_and_data_signal_quality_"
    "decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_AND_DATA_SIGNAL_QUALITY_"
    "DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_AND_DATA_SIGNAL_QUALITY_"
    "DECISION_BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = "PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY"
NEXT_ROUTE = "TRADING-2402_Dynamic_Strategy_Data_PIT_And_Signal_Quality_Gap_Review"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2396",
    "TRADING-2397",
    "TRADING-2398",
    "TRADING-2399",
    "TRADING-2400",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_PLATEAU_DECISION_ONLY_NO_FRESH_MARKET_DATA"
)

BASE_CANDIDATE = m2400.BASE_CANDIDATE
BEST_TARGETED_VARIANT = m2400.BEST_TARGETED_VARIANT
EXPECTED_DECISION_FROM_2399 = m2400.EXPECTED_DECISION_FROM_2399
EXPECTED_OBSERVATION_PREVIEW_COUNT = 0
DEFAULT_NEXT_DIRECTION_OPTION = (
    "OPTION_B_PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY"
)
SECONDARY_RECOMMENDED_OPTION = "OPTION_C_BUILD_THRESHOLD_META_DATASET_FIRST"

PLATEAU_CRITERIA: tuple[tuple[str, bool], ...] = (
    ("no_observation_preview_candidate_after_targeted_retest", True),
    ("best_variant_still_below_observation_preview", True),
    ("targeted_improvement_value_retained", True),
    ("more_local_variant_search_has_diminishing_return_risk", True),
    ("data_signal_quality_review_recommended", True),
    ("threshold_meta_dataset_recommended", True),
)
EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
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
    "new_scoring",
)

DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2400_OWNER_REVIEW_DECISION_PATH = (
    m2400.DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2400_OBSERVATION_NON_APPROVAL_RECORD_PATH = (
    m2400.DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "observation_non_approval_record.json"
)
DEFAULT_SOURCE_2400_TARGETED_IMPROVEMENT_VALUE_SUMMARY_PATH = (
    m2400.DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "targeted_improvement_value_summary.json"
)
DEFAULT_SOURCE_2400_NEXT_ROUTE_PATH = (
    m2400.DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "next_route.json"
)
DEFAULT_SOURCE_2399_TARGETED_RETEST_RESULT_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "targeted_gate_evidence_retest_result.json"
)
DEFAULT_SOURCE_2399_TARGETED_VARIANT_RANKING_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "targeted_variant_ranking.json"
)
DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "gate_evidence_plan_result.json"
)
DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH = (
    m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH = (
    m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    / "recombination_retest_result.json"
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


def run_dynamic_strategy_recombination_line_plateau_decision(
    *,
    source_owner_review_decision_2400_path: Path = (
        DEFAULT_SOURCE_2400_OWNER_REVIEW_DECISION_PATH
    ),
    source_observation_non_approval_record_2400_path: Path = (
        DEFAULT_SOURCE_2400_OBSERVATION_NON_APPROVAL_RECORD_PATH
    ),
    source_targeted_improvement_value_summary_2400_path: Path = (
        DEFAULT_SOURCE_2400_TARGETED_IMPROVEMENT_VALUE_SUMMARY_PATH
    ),
    source_next_route_2400_path: Path = DEFAULT_SOURCE_2400_NEXT_ROUTE_PATH,
    source_targeted_retest_result_2399_path: Path = (
        DEFAULT_SOURCE_2399_TARGETED_RETEST_RESULT_PATH
    ),
    source_targeted_variant_ranking_2399_path: Path = (
        DEFAULT_SOURCE_2399_TARGETED_VARIANT_RANKING_PATH
    ),
    source_decision_update_2399_path: Path = DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH,
    source_gate_evidence_plan_result_2398_path: Path = (
        DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH
    ),
    source_owner_review_decision_2397_path: Path = (
        DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH
    ),
    source_recombination_retest_result_2396_path: Path = (
        DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_owner_review_decision_2400_path=source_owner_review_decision_2400_path,
        source_observation_non_approval_record_2400_path=(
            source_observation_non_approval_record_2400_path
        ),
        source_targeted_improvement_value_summary_2400_path=(
            source_targeted_improvement_value_summary_2400_path
        ),
        source_next_route_2400_path=source_next_route_2400_path,
        source_targeted_retest_result_2399_path=source_targeted_retest_result_2399_path,
        source_targeted_variant_ranking_2399_path=(
            source_targeted_variant_ranking_2399_path
        ),
        source_decision_update_2399_path=source_decision_update_2399_path,
        source_gate_evidence_plan_result_2398_path=(
            source_gate_evidence_plan_result_2398_path
        ),
        source_owner_review_decision_2397_path=source_owner_review_decision_2397_path,
        source_recombination_retest_result_2396_path=(
            source_recombination_retest_result_2396_path
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
    sources["source_ready_for_plateau_decision"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "owner_review_decision_2400": m2400.READY_STATUS,
        "observation_non_approval_record_2400": m2400.READY_STATUS,
        "targeted_improvement_value_summary_2400": m2400.READY_STATUS,
        "next_route_2400": m2400.READY_STATUS,
        "targeted_retest_result_2399": m2399.READY_STATUS,
        "targeted_variant_ranking_2399": m2399.READY_STATUS,
        "decision_update_2399": m2399.READY_STATUS,
        "gate_evidence_plan_result_2398": m2398.READY_STATUS,
        "owner_review_decision_2397": m2397.READY_STATUS,
        "recombination_retest_result_2396": m2396.READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )
    _validate_2400_owner_decision(sources, errors)
    _validate_2399_targeted_retest(sources, errors)
    _validate_history_context(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2400_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2400"))
    non_approval_doc = _as_mapping(sources.get("observation_non_approval_record_2400"))
    non_approval = _as_mapping(non_approval_doc.get("observation_non_approval_record"))
    value_doc = _as_mapping(sources.get("targeted_improvement_value_summary_2400"))
    value_summary = _as_mapping(value_doc.get("targeted_improvement_value_summary"))
    route_doc = _as_mapping(sources.get("next_route_2400"))
    route = _as_mapping(route_doc.get("next_route"))

    if owner.get("owner_decision") != m2400.OWNER_DECISION:
        errors.append("2400 owner decision mismatch")
    if owner.get("base_candidate") != BASE_CANDIDATE:
        errors.append("2400 base candidate mismatch")
    if owner.get("best_targeted_variant") != BEST_TARGETED_VARIANT:
        errors.append("2400 best targeted variant mismatch")
    if owner.get("best_targeted_variant_decision_from_2399") != (
        EXPECTED_DECISION_FROM_2399
    ):
        errors.append("2400 best targeted variant decision mismatch")
    if owner.get("observation_preview_candidates_count") != (
        EXPECTED_OBSERVATION_PREVIEW_COUNT
    ):
        errors.append("2400 observation preview count mismatch")
    if owner.get("targeted_improvement_value_retained") is not True:
        errors.append("2400 targeted improvement value was not retained")
    if owner.get("plateau_review_required") is not True:
        errors.append("2400 plateau review was not required")
    if owner.get("data_signal_quality_review_recommended") is not True:
        errors.append("2400 data/signal quality review was not recommended")
    if owner.get("threshold_meta_dataset_recommended") is not True:
        errors.append("2400 threshold meta dataset was not recommended")
    if owner.get("recommended_next_research_task") != m2400.NEXT_ROUTE:
        errors.append("2400 route did not point to TRADING-2401")
    if owner.get("research_only_observation_approved") is True:
        errors.append("2400 unexpectedly approved research-only observation")

    if non_approval.get("record_ready") is not True:
        errors.append("2400 observation non-approval record is not ready")
    if non_approval.get("research_only_observation_approved") is True:
        errors.append("2400 non-approval record unexpectedly approved observation")
    if value_summary.get("research_value") is not True:
        errors.append("2400 value summary did not retain research value")
    if value_summary.get("observation_ready") is not False:
        errors.append("2400 value summary unexpectedly observation-ready")
    if route.get("recommended_next_research_task") != m2400.NEXT_ROUTE:
        errors.append("2400 next route artifact mismatch")


def _validate_2399_targeted_retest(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("targeted_retest_result_2399"))
    ranking_doc = _as_mapping(sources.get("targeted_variant_ranking_2399"))
    decision_doc = _as_mapping(sources.get("decision_update_2399"))
    decision_update = _as_mapping(decision_doc.get("decision_update"))
    if retest.get("best_targeted_variant") != BEST_TARGETED_VARIANT:
        errors.append("2399 best targeted variant mismatch")
    if retest.get("best_targeted_variant_decision") != EXPECTED_DECISION_FROM_2399:
        errors.append("2399 best targeted variant decision mismatch")
    if retest.get("observation_preview_candidates_count") != 0:
        errors.append("2399 observation preview count mismatch")
    if retest.get("recommended_next_research_task") != m2399.NEXT_ROUTE:
        errors.append("2399 route did not point to TRADING-2400")
    if ranking_doc.get("best_targeted_variant") != BEST_TARGETED_VARIANT:
        errors.append("2399 ranking best targeted variant mismatch")
    if decision_update.get("research_only_observation_preview_exists") is True:
        errors.append("2399 unexpectedly found observation preview candidate")
    if decision_update.get("observation_preview_candidates_count") != 0:
        errors.append("2399 decision update observation preview count mismatch")


def _validate_history_context(sources: Mapping[str, Any], errors: list[str]) -> None:
    plan_2398 = _as_mapping(sources.get("gate_evidence_plan_result_2398"))
    owner_2397 = _as_mapping(sources.get("owner_review_decision_2397"))
    retest_2396 = _as_mapping(sources.get("recombination_retest_result_2396"))
    if plan_2398.get("recommended_next_research_task") != m2398.NEXT_ROUTE:
        errors.append("2398 route did not point to TRADING-2399")
    if len(_as_list(plan_2398.get("planned_targeted_variants"))) != 6:
        errors.append("2398 planned targeted variant count mismatch")
    if owner_2397.get("owner_decision") != m2397.OWNER_DECISION:
        errors.append("2397 owner decision mismatch")
    if retest_2396.get("best_recombination_candidate") != BASE_CANDIDATE:
        errors.append("2396 best recombination candidate mismatch")
    if retest_2396.get("best_recombination_decision") != m2396.DECISION_OWNER_REVIEW:
        errors.append("2396 best recombination decision mismatch")


def _validate_source_safety(sources: Mapping[str, Any], errors: list[str]) -> None:
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
        "source_ready_for_plateau_decision": bool(
            sources.get("source_ready_for_plateau_decision")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": "NOT_APPLICABLE_PRIOR_ARTIFACT_PLATEAU_DECISION_ONLY",
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
    history = _recombination_line_history(sources)
    plateau_review = _plateau_review(history)
    next_decision = _next_research_direction_decision()
    route = _data_signal_quality_route()
    return {
        "owner_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "base_candidate": BASE_CANDIDATE,
        "best_targeted_variant": BEST_TARGETED_VARIANT,
        "best_targeted_variant_decision_from_2399": EXPECTED_DECISION_FROM_2399,
        "observation_preview_candidates_count": 0,
        "recombination_line_history": history,
        "recombination_line_plateau_review_ready": True,
        "recombination_line_plateau_detected": True,
        "recombination_line_plateau_review": plateau_review,
        "continue_local_targeted_improvement_recommended": False,
        "data_signal_quality_review_recommended": True,
        "pit_coverage_review_recommended": True,
        "regime_labeling_review_recommended": True,
        "threshold_meta_dataset_recommended": True,
        "next_research_direction_decision": next_decision,
        "data_signal_quality_review_scope": _data_signal_quality_review_scope(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "source_findings": _source_findings(sources),
        "guardrail_summary": _guardrail_summary(),
        "data_signal_quality_review_route": route,
        "research_quality_status": (
            "RECOMBINATION_LINE_PLATEAU_DETECTED_DATA_SIGNAL_QUALITY_REVIEW_REQUIRED"
        ),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "owner_decision_recorded": False,
        "owner_decision": None,
        "base_candidate": None,
        "best_targeted_variant": None,
        "best_targeted_variant_decision_from_2399": None,
        "observation_preview_candidates_count": None,
        "recombination_line_history": {},
        "recombination_line_plateau_review_ready": False,
        "recombination_line_plateau_detected": False,
        "recombination_line_plateau_review": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "continue_local_targeted_improvement_recommended": False,
        "data_signal_quality_review_recommended": False,
        "pit_coverage_review_recommended": False,
        "regime_labeling_review_recommended": False,
        "threshold_meta_dataset_recommended": False,
        "next_research_direction_decision": {},
        "data_signal_quality_review_scope": _data_signal_quality_review_scope(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "source_findings": _source_findings(sources),
        "guardrail_summary": _guardrail_summary(),
        "data_signal_quality_review_route": {},
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
        "recommended_next_research_task": None,
    }


def _recombination_line_history(sources: Mapping[str, Any]) -> dict[str, Any]:
    retest_2396 = _as_mapping(sources.get("recombination_retest_result_2396"))
    owner_2397 = _as_mapping(sources.get("owner_review_decision_2397"))
    plan_2398 = _as_mapping(sources.get("gate_evidence_plan_result_2398"))
    retest_2399 = _as_mapping(sources.get("targeted_retest_result_2399"))
    owner_2400 = _as_mapping(sources.get("owner_review_decision_2400"))
    return {
        "TRADING-2396": {
            "result": "recombination candidate retest 已完成",
            "best_candidate": retest_2396.get("best_recombination_candidate"),
            "decision": retest_2396.get("best_recombination_decision"),
        },
        "TRADING-2397": {
            "result": "owner review decision 已记录",
            "owner_decision": owner_2397.get("owner_decision"),
        },
        "TRADING-2398": {
            "result": "gate evidence 与 targeted improvement plan 已形成",
            "planned_targeted_variants": len(
                _as_list(plan_2398.get("planned_targeted_variants"))
            ),
        },
        "TRADING-2399": {
            "result": "targeted gate evidence retest 已完成",
            "best_targeted_variant": retest_2399.get("best_targeted_variant"),
            "decision": retest_2399.get("best_targeted_variant_decision"),
            "observation_preview_candidates_count": retest_2399.get(
                "observation_preview_candidates_count"
            ),
        },
        "TRADING-2400": {
            "result": "owner non-approval decision 已记录",
            "owner_decision": owner_2400.get("owner_decision"),
        },
    }


def _plateau_review(history: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_recombination_plateau_review.v1",
        "recombination_line_plateau_detected": True,
        "plateau_scope": "growth_tilt_lower_turnover_guarded_transfer_line",
        "recombination_line_history": dict(history),
        "plateau_criteria": {
            key: {"expected": expected, "observed": expected, "passed": True}
            for key, expected in PLATEAU_CRITERIA
        },
        "plateau_evidence": [
            "2396 只产生 OWNER_REVIEW_REQUIRED，未产生 observation preview",
            "2397 保留 owner review，未批准 observation",
            "2398 规划了 6 个 targeted variants",
            "2399 最佳 targeted variant 仍为 CONTINUE_TARGETED_IMPROVEMENT",
            "2399 observation preview candidate count 仍为 0",
            "2400 明确要求进入 plateau / data-signal quality review",
        ],
        "primary_blockers": [
            "targeted retest 后仍没有 observation preview candidate",
            "继续做局部 variant 搜索存在边际收益递减风险",
            "data / PIT / signal quality 和 regime labeling 可能限制证据质量",
            "threshold calibration 仍缺少 meta-dataset",
        ],
    }


def _next_research_direction_decision() -> dict[str, Any]:
    return {
        "record_ready": True,
        "owner_decision": OWNER_DECISION,
        "recommended_default_option": DEFAULT_NEXT_DIRECTION_OPTION,
        "secondary_recommended_option": SECONDARY_RECOMMENDED_OPTION,
        "continue_local_targeted_improvement_recommended": False,
        "data_signal_quality_review_recommended": True,
        "pit_coverage_review_recommended": True,
        "regime_labeling_review_recommended": True,
        "threshold_meta_dataset_recommended": True,
        "decision_options": {
            "OPTION_A_CONTINUE_LOCAL_TARGETED_IMPROVEMENT": {
                "recommended": False,
                "risk": "边际收益递减风险高",
                "meaning": "继续生成小幅 valid-until strict / guarded transfer variants",
            },
            DEFAULT_NEXT_DIRECTION_OPTION: {
                "recommended": True,
                "meaning": (
                    "暂停当前 recombination line，复盘 data quality、PIT coverage、"
                    "signal quality 和 regime labeling"
                ),
            },
            SECONDARY_RECOMMENDED_OPTION: {
                "recommended": True,
                "meaning": (
                    "先构建 threshold calibration meta-dataset，再判断 candidate gates "
                    "是否过严"
                ),
            },
            "OPTION_D_REVISIT_SIGNAL_CONSTRUCTION_FRAMEWORK": {
                "recommended": False,
                "meaning": (
                    "回到 indicator -> signal -> weight mapping，评估底层 signal quality"
                ),
            },
            "OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW": {
                "recommended": False,
                "meaning": "暂时停止 dynamic strategy research line",
            },
        },
    }


def _data_signal_quality_review_scope() -> dict[str, list[str]]:
    return {
        "data_quality": [
            "评估 PASS_WITH_WARNINGS 警告是否影响 dynamic strategy research",
            "复盘 cached market data 覆盖范围与 stale data 风险",
            "复盘 survivorship、lookahead 与 corporate-action 处理",
            "复盘 missing values 与 source reconciliation",
        ],
        "PIT_coverage": [
            "评估当前 PIT approximation 是否足以支持 signal validation",
            "识别缺少 point-in-time history 的 features 或 signals",
            "判断是否需要更多 history artifacts 或外部数据源",
        ],
        "signal_quality": [
            "复盘 growth_tilt_engine signal source stability",
            "复盘 valid_until_window signal expiry assumptions",
            "复盘 signal-to-execution lag accuracy",
            "复盘 stale signal detection coverage",
        ],
        "regime_labeling": [
            "复盘 risk_on / risk_off / high_vol / low_vol / recovery labels",
            "检验 regime expectation 是否过粗",
            "评估用 regime_expectation_score 替代 regime_slice_pass_rate",
        ],
        "threshold_meta_dataset": [
            "校准 time_slice_pass_rate threshold",
            "校准 regime_expectation_score threshold",
            "校准 drawdown materiality threshold",
        ],
    }


def _data_signal_quality_route() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": (
            "当前 recombination line 已在没有 observation preview 的状态下进入 plateau；"
            "下一步应复盘 data、PIT、signal quality、regime labeling 和 threshold "
            "calibration，而不是继续生成局部 variants"
        ),
        "owner_decision": OWNER_DECISION,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
    }


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    owner_2400 = _as_mapping(sources.get("owner_review_decision_2400"))
    retest_2399 = _as_mapping(sources.get("targeted_retest_result_2399"))
    plan_2398 = _as_mapping(sources.get("gate_evidence_plan_result_2398"))
    owner_2397 = _as_mapping(sources.get("owner_review_decision_2397"))
    retest_2396 = _as_mapping(sources.get("recombination_retest_result_2396"))
    return {
        "trading_2400": {
            "status": owner_2400.get("status"),
            "owner_decision": owner_2400.get("owner_decision"),
            "plateau_review_required": owner_2400.get("plateau_review_required"),
            "data_signal_quality_review_recommended": owner_2400.get(
                "data_signal_quality_review_recommended"
            ),
            "threshold_meta_dataset_recommended": owner_2400.get(
                "threshold_meta_dataset_recommended"
            ),
        },
        "trading_2399": {
            "status": retest_2399.get("status"),
            "best_targeted_variant": retest_2399.get("best_targeted_variant"),
            "best_targeted_variant_decision": retest_2399.get(
                "best_targeted_variant_decision"
            ),
            "observation_preview_candidates_count": retest_2399.get(
                "observation_preview_candidates_count"
            ),
        },
        "trading_2398": {
            "status": plan_2398.get("status"),
            "planned_targeted_variants": len(
                _as_list(plan_2398.get("planned_targeted_variants"))
            ),
        },
        "trading_2397": {
            "status": owner_2397.get("status"),
            "owner_decision": owner_2397.get("owner_decision"),
        },
        "trading_2396": {
            "status": retest_2396.get("status"),
            "best_recombination_candidate": retest_2396.get(
                "best_recombination_candidate"
            ),
            "best_recombination_decision": retest_2396.get(
                "best_recombination_decision"
            ),
        },
    }


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "PLATEAU_AND_NEXT_DIRECTION_DECISION_RECORD_ONLY",
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


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "json_path": str(output_root / "plateau_decision_result.json"),
        "recombination_plateau_review_json": str(
            output_root / "recombination_plateau_review.json"
        ),
        "next_research_direction_decision_json": str(
            output_root / "next_research_direction_decision.json"
        ),
        "data_signal_quality_review_route_json": str(
            output_root / "data_signal_quality_review_route.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_recombination_line_plateau_decision.md"
        ),
        "recombination_plateau_review_markdown": str(
            docs_root / "dynamic_strategy_recombination_plateau_review.md"
        ),
        "next_research_direction_markdown": str(
            docs_root / "dynamic_strategy_data_signal_quality_next_direction.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2402_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["recombination_plateau_review_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_plateau_review",
            "schema_version": "dynamic_strategy_recombination_plateau_review.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "recombination_line_plateau_review": payload.get(
                "recombination_line_plateau_review", {}
            ),
            "recombination_line_plateau_detected": payload.get(
                "recombination_line_plateau_detected"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["next_research_direction_decision_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_data_signal_quality_next_direction",
            "schema_version": (
                "dynamic_strategy_data_signal_quality_next_direction.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "next_research_direction_decision": payload.get(
                "next_research_direction_decision", {}
            ),
            "data_signal_quality_review_scope": payload.get(
                "data_signal_quality_review_scope", {}
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["data_signal_quality_review_route_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_2402_route",
            "schema_version": "dynamic_strategy_2402_route.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "data_signal_quality_review_route": payload.get(
                "data_signal_quality_review_route", {}
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
        Path(paths["recombination_plateau_review_markdown"]),
        _plateau_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["next_research_direction_markdown"]),
        _next_direction_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy recombination line plateau 决策",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as_of：`{payload.get('as_of')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- base candidate：`{payload.get('base_candidate')}`",
            f"- best targeted variant：`{payload.get('best_targeted_variant')}`",
            "- recombination line plateau detected："
            f"`{payload.get('recombination_line_plateau_detected')}`",
            "- continue local targeted improvement recommended："
            f"`{payload.get('continue_local_targeted_improvement_recommended')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## TRADING-2396 至 TRADING-2400 的来源结论",
            "",
            _json_block(payload.get("source_findings", {})),
            "",
            "## Recombination line 历史",
            "",
            _json_block(payload.get("recombination_line_history", {})),
            "",
            "## Plateau 评估",
            "",
            _json_block(payload.get("recombination_line_plateau_review", {})),
            "",
            "## 为什么仍不批准 observation",
            "",
            "- targeted retest 后仍没有 observation preview candidate。",
            "- best targeted variant 仍停留在 CONTINUE_TARGETED_IMPROVEMENT。",
            "- 2400 owner decision 明确保留 non-approval。",
            "- 下一步是 evidence-quality review，不是 execution exposure。",
            "",
            "## Targeted improvement value 保留",
            "",
            "- valid-until strict direction 仍是有研究价值的 reference。",
            "- 保留研究价值不等于 observation readiness。",
            "",
            "## 下一步方向选项",
            "",
            _json_block(payload.get("next_research_direction_decision", {})),
            "",
            "## 推荐下一步方向",
            "",
            f"- `{DEFAULT_NEXT_DIRECTION_OPTION}`",
            f"- secondary：`{SECONDARY_RECOMMENDED_OPTION}`",
            "",
            "## Data / PIT / signal quality 复盘范围",
            "",
            _json_block(payload.get("data_signal_quality_review_scope", {})),
            "",
            "## 明确不批准事项",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
            "## 推荐下一任务",
            "",
            f"- `{payload.get('recommended_next_research_task')}`",
            "",
        ]
    )


def _plateau_markdown(payload: Mapping[str, Any]) -> str:
    review = _as_mapping(payload.get("recombination_line_plateau_review"))
    return "\n".join(
        [
            "# Dynamic strategy recombination plateau 复盘",
            "",
            f"- status：`{payload.get('status')}`",
            "- plateau detected："
            f"`{payload.get('recombination_line_plateau_detected')}`",
            f"- plateau scope：`{review.get('plateau_scope')}`",
            "",
            "## Plateau 判定条件",
            "",
            _json_block(review.get("plateau_criteria", {})),
            "",
            "## Plateau 证据",
            "",
            *[f"- {item}" for item in review.get("plateau_evidence", [])],
            "",
            "## 主要阻断因素",
            "",
            *[f"- {item}" for item in review.get("primary_blockers", [])],
            "",
        ]
    )


def _next_direction_markdown(payload: Mapping[str, Any]) -> str:
    direction = _as_mapping(payload.get("next_research_direction_decision"))
    return "\n".join(
        [
            "# Dynamic strategy data signal quality 下一步方向",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{direction.get('owner_decision')}`",
            f"- recommended default option：`{direction.get('recommended_default_option')}`",
            f"- secondary option：`{direction.get('secondary_recommended_option')}`",
            "",
            "## 复盘范围",
            "",
            _json_block(payload.get("data_signal_quality_review_scope", {})),
            "",
            "## 决策选项",
            "",
            _json_block(direction.get("decision_options", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    route = _as_mapping(payload.get("data_signal_quality_review_route"))
    return "\n".join(
        [
            "# Dynamic strategy 2402 路由",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{route.get('recommended_next_research_task')}`",
            f"- route reason：{route.get('route_reason')}",
            f"- owner decision：`{route.get('owner_decision')}`",
            "- research-only observation approved："
            f"`{route.get('research_only_observation_approved')}`",
            f"- paper-shadow enabled：`{route.get('paper_shadow_enabled')}`",
            f"- production enabled：`{route.get('production_enabled')}`",
            f"- broker action enabled：`{route.get('broker_action_enabled')}`",
            "",
        ]
    )


def _source_document_names() -> tuple[str, ...]:
    return (
        "owner_review_decision_2400",
        "observation_non_approval_record_2400",
        "targeted_improvement_value_summary_2400",
        "next_route_2400",
        "targeted_retest_result_2399",
        "targeted_variant_ranking_2399",
        "decision_update_2399",
        "gate_evidence_plan_result_2398",
        "owner_review_decision_2397",
        "recombination_retest_result_2396",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []
