from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_pit_coverage_matrix_reusable_implementation as m2405
import ai_trading_system.dynamic_strategy_pit_coverage_signal_construction_review as m2403
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
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2406"
TASK_REGISTER_ID = (
    "TRADING-2406_GROWTH_TILT_ENGINE_PIT_AND_SIGNAL_CONSTRUCTION_REMEDIATION_PLAN"
)
REPORT_TYPE = "dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan"
SCHEMA_VERSION = (
    "dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_AND_SIGNAL_CONSTRUCTION_"
    "REMEDIATION_PLAN_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_AND_SIGNAL_CONSTRUCTION_"
    "REMEDIATION_PLAN_BLOCKED_SOURCE"
)
INPUT_UNDER_REVIEW = "growth_tilt_engine"
INPUT_TYPE = "SIGNAL"
CURRENT_SEVERITY = "BLOCKING"
CURRENT_PIT_STATUS = "UNKNOWN_OR_APPROXIMATE_PIT"
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2403", "TRADING-2404", "TRADING-2405")
NEXT_ROUTE = (
    "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_CONFIG_ONLY_NO_FRESH_MARKET_DATA"
)
FOCUSED_GROWTH_TILT_CANDIDATE_ID = (
    "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "clear_growth_tilt_engine_blocking_gap",
    "downgrade_growth_tilt_engine_severity",
    "mark_growth_tilt_engine_true_pit",
    "candidate_search_resume",
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
    "new_strategy_backtest",
    "new_trading_signal",
    "new_scoring",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "growth_tilt_engine_blocking_gap_resolved",
    "growth_tilt_engine_severity_downgraded",
    "candidate_search_allowed",
    "research_only_observation_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "candidate_search_resumed",
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
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
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "new_signal_generated",
    "scoring_run",
)

DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    / "implementation_result.json"
)
DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    / "pit_input_registry_snapshot.json"
)
DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    / "pit_coverage_matrix.json"
)
DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    / "pit_gate_result.json"
)
DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    / "pit_blocker_summary.json"
)
DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    / "pit_remediation_routes.json"
)
DEFAULT_SOURCE_2403_PIT_MATRIX_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "pit_coverage_matrix.json"
)
DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "signal_construction_review.json"
)
DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "remediation_matrix.json"
)
DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH
)
DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "equal_risk_growth_tilt_candidate_registry.yaml"
)
DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "strategy_execution_policy_registry.yaml"
)


def run_dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan(
    *,
    source_2405_implementation_path: Path = DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Path = DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Path = DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Path = DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Path = DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Path = DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2403_pit_matrix_path: Path = DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_signal_construction_review_path: Path = (
        DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH
    ),
    source_2403_remediation_matrix_path: Path = DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    pit_input_registry_path: Path = DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    growth_tilt_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    execution_policy_registry_path: Path = DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2405_implementation_path=source_2405_implementation_path,
        source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
        source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        source_2405_remediation_routes_path=source_2405_remediation_routes_path,
        source_2403_pit_matrix_path=source_2403_pit_matrix_path,
        source_2403_signal_construction_review_path=(
            source_2403_signal_construction_review_path
        ),
        source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
        pit_input_registry_path=pit_input_registry_path,
        growth_tilt_config_path=growth_tilt_config_path,
        execution_policy_registry_path=execution_policy_registry_path,
    )
    validation_errors = _validate_sources(sources)
    ready = not validation_errors
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        sources=sources,
        as_of_date=as_of_date,
        source_validation_errors=validation_errors,
    )
    if ready:
        payload.update(_ready_sections(sources))
    else:
        payload.update(_blocked_sections())
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_2405_implementation_path: Path,
    source_2405_registry_snapshot_path: Path,
    source_2405_pit_coverage_matrix_path: Path,
    source_2405_pit_gate_result_path: Path,
    source_2405_blocker_summary_path: Path,
    source_2405_remediation_routes_path: Path,
    source_2403_pit_matrix_path: Path,
    source_2403_signal_construction_review_path: Path,
    source_2403_remediation_matrix_path: Path,
    pit_input_registry_path: Path,
    growth_tilt_config_path: Path,
    execution_policy_registry_path: Path,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "implementation_2405": _load_json_document(source_2405_implementation_path),
        "registry_snapshot_2405": _load_json_document(
            source_2405_registry_snapshot_path
        ),
        "pit_matrix_2405": _load_json_document(source_2405_pit_coverage_matrix_path),
        "pit_gate_result_2405": _load_json_document(source_2405_pit_gate_result_path),
        "blocker_summary_2405": _load_json_document(source_2405_blocker_summary_path),
        "remediation_routes_2405": _load_json_document(
            source_2405_remediation_routes_path
        ),
        "pit_matrix_2403": _load_json_document(source_2403_pit_matrix_path),
        "signal_construction_review_2403": _load_json_document(
            source_2403_signal_construction_review_path
        ),
        "remediation_matrix_2403": _load_json_document(
            source_2403_remediation_matrix_path
        ),
        "pit_input_registry_config": _load_yaml_document(pit_input_registry_path),
        "growth_tilt_config": _load_yaml_document(growth_tilt_config_path),
        "execution_policy_registry": _load_yaml_document(execution_policy_registry_path),
    }
    sources["source_paths"] = {
        "implementation_2405": str(source_2405_implementation_path),
        "registry_snapshot_2405": str(source_2405_registry_snapshot_path),
        "pit_matrix_2405": str(source_2405_pit_coverage_matrix_path),
        "pit_gate_result_2405": str(source_2405_pit_gate_result_path),
        "blocker_summary_2405": str(source_2405_blocker_summary_path),
        "remediation_routes_2405": str(source_2405_remediation_routes_path),
        "pit_matrix_2403": str(source_2403_pit_matrix_path),
        "signal_construction_review_2403": str(
            source_2403_signal_construction_review_path
        ),
        "remediation_matrix_2403": str(source_2403_remediation_matrix_path),
        "pit_input_registry_config": str(pit_input_registry_path),
        "growth_tilt_config": str(growth_tilt_config_path),
        "execution_policy_registry": str(execution_policy_registry_path),
    }
    return sources


def _load_yaml_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    document = safe_load_yaml_path(path)
    if not isinstance(document, Mapping):
        return {"_invalid": True, "_path": str(path), "_observed_type": type(document).__name__}
    return dict(document)


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_statuses = {
        "implementation_2405": m2405.READY_STATUS,
        "registry_snapshot_2405": m2405.READY_STATUS,
        "pit_matrix_2405": m2405.READY_STATUS,
        "pit_gate_result_2405": m2405.READY_STATUS,
        "blocker_summary_2405": m2405.READY_STATUS,
        "remediation_routes_2405": m2405.READY_STATUS,
        "pit_matrix_2403": m2403.READY_STATUS,
        "signal_construction_review_2403": m2403.READY_STATUS,
        "remediation_matrix_2403": m2403.READY_STATUS,
    }
    for source_name, expected in expected_statuses.items():
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing source artifact {source.get('_path')}")
        elif source.get("status") != expected:
            errors.append(
                f"{source_name}: expected status {expected}, observed {source.get('status')}"
            )
    for source_name in (
        "pit_input_registry_config",
        "growth_tilt_config",
        "execution_policy_registry",
    ):
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing config {source.get('_path')}")
        if source.get("_invalid"):
            errors.append(f"{source_name}: invalid config object {source.get('_path')}")

    implementation_2405 = _as_mapping(sources.get("implementation_2405"))
    if implementation_2405.get("recommended_next_research_task") != m2405.NEXT_ROUTE:
        errors.append("2405 next route mismatch")
    if implementation_2405.get("candidate_search_resumed") is not False:
        errors.append("2405 candidate_search_resumed must be false")
    if implementation_2405.get("candidate_search_allowed") is not False:
        errors.append("2405 candidate_search_allowed must be false")
    if implementation_2405.get("research_only_observation_allowed") is not False:
        errors.append("2405 research_only_observation_allowed must be false")
    if implementation_2405.get("paper_shadow_allowed") is not False:
        errors.append("2405 paper_shadow_allowed must be false")
    if implementation_2405.get("production_allowed") is not False:
        errors.append("2405 production_allowed must be false")
    for field in m2405.SAFETY_FALSE_FIELDS:
        if implementation_2405.get(field) is True:
            errors.append(f"2405 safety field must be false: {field}")
    if implementation_2405.get("broker_action") not in (None, "none"):
        errors.append("2405 broker_action must be none")
    if INPUT_UNDER_REVIEW not in _as_list(implementation_2405.get("blocking_gaps")):
        errors.append("2405 blocking gaps missing growth_tilt_engine")

    gate = _as_mapping(
        _as_mapping(sources.get("pit_gate_result_2405")).get("pit_gate_result")
    )
    if gate.get("candidate_search_allowed") is not False:
        errors.append("2405 PIT gate candidate_search_allowed must be false")
    if "BLOCKING_GAP_GROWTH_TILT_ENGINE" not in _as_list(gate.get("blockers")):
        errors.append("2405 PIT gate missing BLOCKING_GAP_GROWTH_TILT_ENGINE")

    blocker_summary = _as_mapping(
        _as_mapping(sources.get("blocker_summary_2405")).get("pit_blocker_summary")
    )
    blocker_details = _as_mapping(blocker_summary.get("blocking_gap_details"))
    growth_blocker = _as_mapping(blocker_details.get(INPUT_UNDER_REVIEW))
    if growth_blocker.get("severity") != CURRENT_SEVERITY:
        errors.append("2405 blocker summary does not keep growth_tilt_engine BLOCKING")
    if growth_blocker.get("candidate_search_blocker") is not True:
        errors.append("growth_tilt_engine must remain candidate search blocker")

    registry_entry = _registry_entry(
        _as_mapping(sources.get("pit_input_registry_config")),
        INPUT_UNDER_REVIEW,
    )
    if registry_entry.get("severity") != CURRENT_SEVERITY:
        errors.append("PIT input registry does not keep growth_tilt_engine BLOCKING")
    if registry_entry.get("candidate_search_blocker") is not True:
        errors.append("PIT input registry growth_tilt_engine must block candidate search")

    if not _pit_row(sources, INPUT_UNDER_REVIEW):
        errors.append("2405/2403 PIT matrix missing growth_tilt_engine row")
    signal_review = _growth_signal_review(sources)
    if not signal_review:
        errors.append("2403 signal construction review missing growth_tilt_engine")

    execution_policy = _execution_policy_for_focused_candidate(
        _as_mapping(sources.get("execution_policy_registry"))
    )
    if not execution_policy:
        errors.append("execution policy registry missing focused growth tilt candidate")
    growth_family = _growth_tilt_candidate_family(
        _as_mapping(sources.get("growth_tilt_config"))
    )
    if not growth_family:
        errors.append("growth tilt config missing vol_target_growth_tilt family")
    return errors


def _base_payload(
    *,
    status: str,
    sources: Mapping[str, Any],
    as_of_date: date | None,
    source_validation_errors: list[str],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat() if as_of_date else None,
        "market_regime": AI_REGIME_SUMMARY["market_regime"],
        "market_regime_summary": dict(AI_REGIME_SUMMARY),
        "source_tasks": list(SOURCE_TASKS),
        "source_paths": dict(_as_mapping(sources.get("source_paths"))),
        "source_validation_errors": source_validation_errors,
        "input_under_review": INPUT_UNDER_REVIEW,
        "input_type": INPUT_TYPE,
        "current_severity": CURRENT_SEVERITY,
        "current_pit_status": CURRENT_PIT_STATUS,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "candidate_search_allowed": False,
        "research_only_observation_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "candidate_search_resumed": False,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "observation_approved": False,
        "paper_shadow_approved": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_approved": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_approved": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "production_approved": False,
        "production_enabled": False,
        "broker_action": "none",
        "broker_action_enabled": False,
        "order_generated": False,
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    current_blocker = _current_blocker_record(sources)
    inventory = _build_source_feature_inventory(sources)
    pit_risk_audit = _build_pit_risk_audit()
    signal_gap_analysis = _build_signal_construction_gap_analysis(sources, inventory)
    remediation_plan = _build_remediation_plan()
    downgrade_conditions = _build_severity_downgrade_conditions()
    validation_plan = _build_validation_plan()
    return {
        "current_blocker": current_blocker,
        "source_feature_inventory_ready": True,
        "pit_risk_audit_ready": True,
        "signal_construction_gap_analysis_ready": True,
        "remediation_plan_ready": True,
        "growth_tilt_engine_remediation_plan_ready": True,
        "severity_downgrade_conditions_ready": True,
        "validation_plan_ready": True,
        "growth_tilt_engine_validation_plan_ready": True,
        "source_feature_inventory": inventory,
        "source_feature_inventory_count": len(inventory),
        "pit_risk_audit": pit_risk_audit,
        "pit_risk_audit_count": len(_as_list(pit_risk_audit.get("risks"))),
        "signal_construction_gap_analysis": signal_gap_analysis,
        "remediation_plan": remediation_plan,
        "severity_downgrade_conditions": downgrade_conditions,
        "validation_plan": validation_plan,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "valid_until_window is the second current BLOCKING PIT gap; complete "
            "both blocker remediation plans before implementation."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "current_blocker": {
            "input_id": INPUT_UNDER_REVIEW,
            "input_type": INPUT_TYPE,
            "severity": CURRENT_SEVERITY,
            "pit_status": CURRENT_PIT_STATUS,
        },
        "source_feature_inventory_ready": False,
        "pit_risk_audit_ready": False,
        "signal_construction_gap_analysis_ready": False,
        "remediation_plan_ready": False,
        "growth_tilt_engine_remediation_plan_ready": False,
        "severity_downgrade_conditions_ready": False,
        "validation_plan_ready": False,
        "growth_tilt_engine_validation_plan_ready": False,
        "source_feature_inventory": [],
        "source_feature_inventory_count": 0,
        "pit_risk_audit": {},
        "pit_risk_audit_count": 0,
        "signal_construction_gap_analysis": {},
        "remediation_plan": {},
        "severity_downgrade_conditions": {},
        "validation_plan": {},
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _current_blocker_record(sources: Mapping[str, Any]) -> dict[str, Any]:
    blocker_summary = _as_mapping(
        _as_mapping(sources.get("blocker_summary_2405")).get("pit_blocker_summary")
    )
    details = _as_mapping(blocker_summary.get("blocking_gap_details"))
    growth_blocker = dict(_as_mapping(details.get(INPUT_UNDER_REVIEW)))
    registry_entry = _registry_entry(
        _as_mapping(sources.get("pit_input_registry_config")),
        INPUT_UNDER_REVIEW,
    )
    return {
        "input_id": INPUT_UNDER_REVIEW,
        "input_type": INPUT_TYPE,
        "severity": growth_blocker.get("severity") or registry_entry.get("severity"),
        "pit_status": registry_entry.get("pit_status", CURRENT_PIT_STATUS),
        "pit_confidence": growth_blocker.get("pit_confidence")
        or registry_entry.get("pit_confidence"),
        "candidate_search_blocker": True,
        "observation_blocker": True,
        "paper_shadow_blocker": True,
        "production_blocker": True,
        "risk_flags": list(
            _as_list(growth_blocker.get("risk_flags"))
            or _as_list(registry_entry.get("risk_flags"))
        ),
        "recommended_action": growth_blocker.get("recommended_action")
        or registry_entry.get("recommended_action"),
        "blocker_resolved_in_2406": False,
        "severity_downgraded_in_2406": False,
    }


def _build_source_feature_inventory(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    growth_config = _as_mapping(sources.get("growth_tilt_config"))
    execution_config = _as_mapping(sources.get("execution_policy_registry"))
    policy = _as_mapping(growth_config.get("research_policy"))
    execution_policy = _execution_policy_for_focused_candidate(execution_config)
    signal_policy = _as_mapping(execution_policy.get("signal_policy"))
    vol_target_grid = _as_mapping(
        _as_mapping(policy.get("search_grids")).get("vol_target_growth_tilt")
    )
    moving_average_windows = _as_mapping(policy.get("moving_average_windows"))
    realized_vol_windows = _as_mapping(policy.get("realized_vol_windows"))
    trend_filter_rule = _as_mapping(policy.get("trend_filter_rule"))

    rows = [
        _feature_row_from_pit(
            sources,
            "adjusted_prices",
            fallback_feature_type="MARKET_DATA",
            recommended_action=(
                "Keep validate-data linkage visible and resolve adjustment-basis caveats "
                "before promotion-quality interpretation."
            ),
        ),
        _feature_row_from_pit(
            sources,
            "returns",
            fallback_feature_type="TECHNICAL_FEATURE",
            recommended_action="Record return window start/end and adjusted-price basis.",
        ),
        _feature_row_from_pit(
            sources,
            "volatility_inputs",
            fallback_feature_type="TECHNICAL_FEATURE",
            recommended_action=(
                "Attach rolling volatility window end-date and no-forward-fill "
                "assertion."
            ),
        ),
        _feature_row_from_pit(
            sources,
            "trend_features",
            fallback_feature_type="TECHNICAL_FEATURE",
            recommended_action="Make trend feature windows explicit before observation review.",
        ),
        _feature_row_from_pit(
            sources,
            "drawdown_features",
            fallback_feature_type="TECHNICAL_FEATURE",
            recommended_action=(
                "Separate live-available drawdown inputs from ex-post evaluation "
                "metrics."
            ),
        ),
        {
            "feature_id": "equal_risk_baseline_weights",
            "feature_type": "PORTFOLIO_STATE",
            "source_config_or_artifact": (
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "research_policy.equal_risk"
            ),
            "used_by_growth_tilt_engine": True,
            "as_of_handling": (
                "derived from trailing QQQ/SGOV realized volatility; no standalone "
                "as-of contract"
            ),
            "generated_at_handling": "generated during research run; no feature manifest emitted",
            "lookback_window": f"medium_realized_vol={realized_vol_windows.get('medium', 'TBD')}",
            "forward_window_used": "none intended; requires replay validation",
            "revision_or_backfill_risk": "BACKFILL_RISK_MATERIAL_WITHOUT_FEATURE_MANIFEST",
            "pit_status": "APPROXIMATE_PIT",
            "pit_confidence": "MEDIUM",
            "severity": "MATERIAL",
            "recommended_action": (
                "Emit baseline weight inputs with as_of_date and source_data_cutoff."
            ),
        },
        {
            "feature_id": "target_vol_policy",
            "feature_type": "SIGNAL_CONSTRUCTION_POLICY",
            "source_config_or_artifact": (
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "search_grids.vol_target_growth_tilt"
            ),
            "used_by_growth_tilt_engine": True,
            "as_of_handling": (
                "target-vol weights use trailing volatility in code but lack explicit "
                "as-of metadata"
            ),
            "generated_at_handling": (
                "research-run generated weights; no standalone generated_at field"
            ),
            "lookback_window": _join_list(vol_target_grid.get("vol_lookback"), "TBD"),
            "forward_window_used": (
                "none intended; target-vol replay needs deterministic as-of test"
            ),
            "revision_or_backfill_risk": "BACKFILL_AND_WINDOW_BOUNDARY_RISK",
            "pit_status": "APPROXIMATE_PIT",
            "pit_confidence": "MEDIUM",
            "severity": "MATERIAL",
            "recommended_action": (
                "Define signal_horizon_days and record target-vol input window "
                "boundaries."
            ),
        },
        {
            "feature_id": "risk_on_trend_filter_context",
            "feature_type": "BEHAVIOR_GUARDRAIL_CONTEXT",
            "source_config_or_artifact": (
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "research_policy.trend_filter_rule"
            ),
            "used_by_growth_tilt_engine": True,
            "as_of_handling": (
                "trend / volatility / drawdown thresholds are trailing by design but "
                "not emitted per signal"
            ),
            "generated_at_handling": (
                "policy config exists; signal-time observation metadata missing"
            ),
            "lookback_window": {
                "moving_average_windows": dict(moving_average_windows),
                "realized_vol_percentile_window": trend_filter_rule.get(
                    "realized_vol_percentile_window"
                ),
            },
            "forward_window_used": (
                "none intended; false risk-on guardrail not separately validated"
            ),
            "revision_or_backfill_risk": "REGIME_CONFIRMATION_AND_FALSE_RISK_ON_RISK",
            "pit_status": "APPROXIMATE_PIT",
            "pit_confidence": "LOW",
            "severity": "MATERIAL",
            "recommended_action": "Separate ex-ante risk-on conditions from ex-post regime labels.",
        },
        {
            "feature_id": "execution_signal_validity_policy",
            "feature_type": "EXECUTION_SEMANTIC_DEPENDENCY",
            "source_config_or_artifact": (
                "config/research/strategy_execution_policy_registry.yaml:"
                f"{FOCUSED_GROWTH_TILT_CANDIDATE_ID}.signal_policy"
            ),
            "used_by_growth_tilt_engine": True,
            "as_of_handling": signal_policy.get("signal_observation_time", "TBD"),
            "generated_at_handling": (
                "policy-configured validity exists but no signal artifact generated_at "
                "field"
            ),
            "lookback_window": "not applicable",
            "forward_window_used": signal_policy.get("signal_validity_window_bdays", "TBD"),
            "revision_or_backfill_risk": "STALE_SIGNAL_RISK_IF_VALID_UNTIL_NOT_GROUNDED",
            "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
            "pit_confidence": "LOW",
            "severity": "BLOCKING",
            "recommended_action": "Route valid-from / valid-until semantics to TRADING-2407.",
        },
        {
            "feature_id": "growth_tilt_engine_signal_artifact",
            "feature_type": "SIGNAL_ARTIFACT_CONTRACT",
            "source_config_or_artifact": "missing standalone growth_tilt_engine signal artifact",
            "used_by_growth_tilt_engine": True,
            "as_of_handling": "missing as_of_date field",
            "generated_at_handling": "missing generated_at / source_data_cutoff fields",
            "lookback_window": "TBD_FROM_SIGNAL_IMPLEMENTATION",
            "forward_window_used": "TBD; signal horizon not separately defined",
            "revision_or_backfill_risk": "LOOKAHEAD_BACKFILL_AND_STALE_SIGNAL_RISK",
            "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
            "pit_confidence": "LOW",
            "severity": "BLOCKING",
            "recommended_action": "Implement explicit as-of contract before downgrading blocker.",
        },
    ]
    return rows


def _feature_row_from_pit(
    sources: Mapping[str, Any],
    input_id: str,
    *,
    fallback_feature_type: str,
    recommended_action: str,
) -> dict[str, Any]:
    row = _pit_row(sources, input_id)
    return {
        "feature_id": input_id,
        "feature_type": str(row.get("input_type") or fallback_feature_type).upper(),
        "source_config_or_artifact": row.get("source_artifact_or_config", "TBD"),
        "used_by_growth_tilt_engine": True,
        "as_of_handling": row.get("as_of_handling", "TBD"),
        "generated_at_handling": row.get("generated_at_handling", "TBD"),
        "lookback_window": row.get("used_by_candidate_or_signal", "TBD"),
        "forward_window_used": "none asserted; must be verified by as-of replay",
        "revision_or_backfill_risk": row.get("revision_risk", "UNKNOWN"),
        "pit_status": row.get("point_in_time_status", "UNKNOWN"),
        "pit_confidence": row.get("pit_confidence", "LOW"),
        "severity": row.get("severity", "MATERIAL"),
        "recommended_action": row.get("recommended_action") or recommended_action,
    }


def _build_pit_risk_audit() -> dict[str, Any]:
    risks = [
        {
            "risk_id": "GTE-PIT-LOOKAHEAD-01",
            "risk_category": "LOOKAHEAD_RISK",
            "affected_feature_or_signal": INPUT_UNDER_REVIEW,
            "severity": "BLOCKING",
            "evidence": (
                "source features and signal horizon are not emitted with a "
                "deterministic as-of replay contract"
            ),
            "remediation_required": True,
            "recommended_fix": (
                "Add as_of_date, source_data_cutoff and window end-date to every "
                "growth tilt signal record."
            ),
        },
        {
            "risk_id": "GTE-PIT-REVISION-01",
            "risk_category": "REVISION_RISK",
            "affected_feature_or_signal": "adjusted_prices / returns",
            "severity": "MATERIAL",
            "evidence": "adjusted-price basis remains a caveat from prior data-quality review",
            "remediation_required": True,
            "recommended_fix": (
                "Carry adjusted-price basis and validate-data report link into growth "
                "tilt signal artifacts."
            ),
        },
        {
            "risk_id": "GTE-PIT-BACKFILL-01",
            "risk_category": "BACKFILL_RISK",
            "affected_feature_or_signal": "trend_features / volatility_inputs",
            "severity": "MATERIAL",
            "evidence": (
                "feature-level generated_at and source_data_cutoff are not normalized "
                "into a manifest"
            ),
            "remediation_required": True,
            "recommended_fix": (
                "Generate feature inventory with window start, window end and source "
                "cutoff before replay validation."
            ),
        },
        {
            "risk_id": "GTE-PIT-STALE-01",
            "risk_category": "STALE_SIGNAL_RISK",
            "affected_feature_or_signal": "execution_signal_validity_policy",
            "severity": "BLOCKING",
            "evidence": (
                "signal validity window exists as policy but natural signal expiry is "
                "not grounded"
            ),
            "remediation_required": True,
            "recommended_fix": (
                "Route valid-from / valid-until semantics and no-stale carry-forward "
                "checks to TRADING-2407."
            ),
        },
        {
            "risk_id": "GTE-PIT-ASOF-01",
            "risk_category": "AS_OF_MISSING_RISK",
            "affected_feature_or_signal": "growth_tilt_engine_signal_artifact",
            "severity": "BLOCKING",
            "evidence": (
                "no standalone signal artifact exposes as_of_date, generated_at, "
                "valid_from or valid_until"
            ),
            "remediation_required": True,
            "recommended_fix": "Define the signal artifact schema before any severity downgrade.",
        },
        {
            "risk_id": "GTE-PIT-REGIME-01",
            "risk_category": "REGIME_CONFIRMATION_RISK",
            "affected_feature_or_signal": "risk_on_trend_filter_context",
            "severity": "MATERIAL",
            "evidence": (
                "risk-on behavior and regime pass/failure evidence remain mixed in "
                "prior research artifacts"
            ),
            "remediation_required": True,
            "recommended_fix": (
                "Separate ex-ante trend / volatility conditions from ex-post regime "
                "labels and drawdown evaluation."
            ),
        },
    ]
    return {
        "schema_version": "dynamic_strategy_growth_tilt_engine_pit_risk_audit.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "risks": risks,
        "blocking_risk_count": sum(1 for risk in risks if risk["severity"] == "BLOCKING"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_signal_construction_gap_analysis(
    sources: Mapping[str, Any],
    inventory: list[dict[str, Any]],
) -> dict[str, Any]:
    signal_review = _growth_signal_review(sources)
    execution_policy = _execution_policy_for_focused_candidate(
        _as_mapping(sources.get("execution_policy_registry"))
    )
    signal_policy = _as_mapping(execution_policy.get("signal_policy"))
    return {
        "schema_version": (
            "dynamic_strategy_growth_tilt_engine_signal_construction_gap_analysis.v1"
        ),
        "signal_id": INPUT_UNDER_REVIEW,
        "signal_role": "core_return_engine",
        "source_features": [row["feature_id"] for row in inventory],
        "prior_signal_review_source_features": list(
            _as_list(signal_review.get("source_features"))
        ),
        "signal_construction": {
            "horizon": signal_review.get("signal_horizon", "TBD"),
            "lookback_window": "TBD_FROM_FEATURE_MANIFEST_AND_TARGET_VOL_GRID",
            "rebalance_cadence_assumption": signal_policy.get(
                "signal_validity_window_bdays",
                "valid_until_window",
            ),
            "signal_decay_rule": signal_review.get("valid_until_rule", "TBD"),
            "confidence_score_available": False,
            "valid_from_available": False,
            "valid_until_available": False,
            "standalone_signal_artifact_available": False,
        },
        "pit_risk": {
            "pit_status": CURRENT_PIT_STATUS,
            "lookahead_risk": "MATERIAL_UNTIL_AS_OF_REPLAY_VALIDATED",
            "revision_risk": "MATERIAL_ADJUSTED_PRICE_BASIS_CAVEAT",
            "stale_signal_risk": "BLOCKING_UNTIL_VALID_UNTIL_WINDOW_GROUNDED",
            "backfill_risk": "MATERIAL_UNTIL_FEATURE_MANIFEST_EXISTS",
        },
        "behavior_risk": {
            "false_risk_on_risk": "MATERIAL",
            "high_volatility_drawdown_amplification_risk": "MATERIAL",
            "recovery_lag_risk": "MATERIAL",
            "overfitting_to_ai_cycle_risk": "MATERIAL",
        },
        "gap_conclusion": (
            "growth_tilt_engine is useful research evidence but cannot be treated "
            "as observation-ready until source feature lineage, as-of replay, "
            "signal horizon, confidence and valid-until semantics are remediated."
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_remediation_plan() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_growth_tilt_engine_remediation_plan.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "growth_tilt_engine_blocking_gap_resolved": False,
        "plan_items": [
            {
                "plan_id": "P0_as_of_contract",
                "priority": "P0",
                "goal": "every growth_tilt signal must carry explicit as_of and generated_at",
                "required_fields": [
                    "as_of_date",
                    "generated_at",
                    "source_data_cutoff",
                    "signal_valid_from",
                    "signal_horizon_days",
                ],
                "expected_result": [
                    "reduce UNKNOWN PIT status",
                    "make signal reproducible by as_of date",
                ],
                "implemented_in_2406": False,
            },
            {
                "plan_id": "P0_source_feature_traceability",
                "priority": "P0",
                "goal": "map every growth_tilt source feature to source config and PIT status",
                "required_outputs": [
                    "source_feature_inventory",
                    "feature_pit_status",
                    "lookahead_risk_flag",
                    "revision_risk_flag",
                ],
                "implemented_in_2406": False,
            },
            {
                "plan_id": "P1_signal_horizon_definition",
                "priority": "P1",
                "goal": "define what period growth_tilt signal is intended to forecast",
                "required_decisions": [
                    "horizon_days",
                    "expected_decay",
                    "valid_from",
                    "valid_until_dependency",
                ],
                "implemented_in_2406": False,
            },
            {
                "plan_id": "P1_signal_confidence",
                "priority": "P1",
                "goal": "separate weak growth tilt from strong growth tilt",
                "required_outputs": [
                    "signal_strength_score",
                    "confidence_band",
                    "uncertainty_reason",
                ],
                "implemented_in_2406": False,
            },
            {
                "plan_id": "P1_false_risk_on_guardrail",
                "priority": "P1",
                "goal": (
                    "reduce growth tilt false positives under high volatility or "
                    "recovery traps"
                ),
                "required_outputs": [
                    "high_volatility_condition",
                    "trend_confirmation_condition",
                    "drawdown_state_guardrail",
                ],
                "implemented_in_2406": False,
            },
            {
                "plan_id": "P2_component_revalidation",
                "priority": "P2",
                "goal": (
                    "rerun component-level validation after remediation before "
                    "candidate search"
                ),
                "note": "not part of 2406; candidate search remains blocked",
                "implemented_in_2406": False,
            },
        ],
        "recommended_implementation_task": (
            "TRADING-2408_Growth_Tilt_Engine_PIT_Remediation_Implementation"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_severity_downgrade_conditions() -> dict[str, Any]:
    return {
        "schema_version": (
            "dynamic_strategy_growth_tilt_engine_severity_downgrade_conditions.v1"
        ),
        "input_under_review": INPUT_UNDER_REVIEW,
        "downgrade_executed_in_2406": False,
        "downgrade_from_BLOCKING_to_MATERIAL_requires": [
            "source_feature_inventory_complete",
            "no_known_lookahead_risk_in_required_features",
            "as_of_date_available",
            "generated_at_or_source_cutoff_available",
            "signal_horizon_defined",
            "signal_valid_from_defined",
            "no_unexplained_future_window_dependency",
            "owner_review_recorded",
        ],
        "downgrade_from_MATERIAL_to_APPROVED_APPROXIMATE_PIT_requires": [
            "approximate_PIT_caveats_explicitly_documented",
            "validation_replay_can_reconstruct_signal_as_of",
            "stale_signal_behavior_defined",
            "false_risk_on_risk_reviewed",
            "owner_approval_recorded",
        ],
        "mark_TRUE_PIT_requires": [
            "all_source_features_true_PIT",
            "no_revision_or_backfill_dependency",
            "deterministic_as_of_replay",
            "validation_test_coverage",
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_validation_plan() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_growth_tilt_engine_validation_plan.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "validation_plan_ready": True,
        "checks": [
            "schema validates growth_tilt signal artifact required fields",
            "as-of replay reconstructs signal using only source rows known by as_of_date",
            "feature manifest records window start/end and source_data_cutoff",
            (
                "no feature uses future return, future drawdown recovery or ex-post "
                "regime confirmation"
            ),
            "signal horizon and valid-from are deterministic",
            "valid-until and stale carry-forward behavior are validated after TRADING-2407",
            "false risk-on guardrail is reviewed under high-volatility drawdown slices",
        ],
        "recommended_routes": {
            "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan": {
                "reason": "second blocking gap from PIT gate",
            },
            "TRADING-2408_Growth_Tilt_Engine_PIT_Remediation_Implementation": {
                "reason": "implement growth_tilt as-of contract and source feature inventory",
            },
            "TRADING-2409_Growth_Tilt_Engine_As_Of_Replay_Validation": {
                "reason": "verify deterministic as-of replay and no lookahead risk",
            },
        },
        "candidate_search_remains_blocked": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_outputs(
    payload: dict[str, Any],
    *,
    output_root: Path,
    docs_root: Path,
) -> None:
    paths = {
        "json_path": str(output_root / "remediation_plan_result.json"),
        "source_feature_inventory_json": str(output_root / "source_feature_inventory.json"),
        "pit_risk_audit_json": str(output_root / "pit_risk_audit.json"),
        "signal_construction_gap_analysis_json": str(
            output_root / "signal_construction_gap_analysis.json"
        ),
        "severity_downgrade_conditions_json": str(
            output_root / "severity_downgrade_conditions.json"
        ),
        "validation_plan_json": str(output_root / "validation_plan.json"),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan.md"
        ),
        "source_feature_inventory_markdown": str(
            docs_root / "dynamic_strategy_growth_tilt_engine_source_feature_inventory.md"
        ),
        "pit_risk_audit_markdown": str(
            docs_root / "dynamic_strategy_growth_tilt_engine_pit_risk_audit.md"
        ),
        "remediation_plan_markdown": str(
            docs_root / "dynamic_strategy_growth_tilt_engine_remediation_plan.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2407_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    for path_key, report_type, schema_key, payload_key in (
        (
            "source_feature_inventory_json",
            "dynamic_strategy_growth_tilt_engine_source_feature_inventory",
            "dynamic_strategy_growth_tilt_engine_source_feature_inventory.v1",
            "source_feature_inventory",
        ),
        (
            "pit_risk_audit_json",
            "dynamic_strategy_growth_tilt_engine_pit_risk_audit",
            "dynamic_strategy_growth_tilt_engine_pit_risk_audit.v1",
            "pit_risk_audit",
        ),
        (
            "signal_construction_gap_analysis_json",
            "dynamic_strategy_growth_tilt_engine_signal_construction_gap_analysis",
            "dynamic_strategy_growth_tilt_engine_signal_construction_gap_analysis.v1",
            "signal_construction_gap_analysis",
        ),
        (
            "severity_downgrade_conditions_json",
            "dynamic_strategy_growth_tilt_engine_severity_downgrade_conditions",
            "dynamic_strategy_growth_tilt_engine_severity_downgrade_conditions.v1",
            "severity_downgrade_conditions",
        ),
        (
            "validation_plan_json",
            "dynamic_strategy_growth_tilt_engine_validation_plan",
            "dynamic_strategy_growth_tilt_engine_validation_plan.v1",
            "validation_plan",
        ),
    ):
        write_json_artifact(
            Path(paths[path_key]),
            {
                "task_id": TASK_ID,
                "status": payload.get("status"),
                "report_type": report_type,
                "schema_version": schema_key,
                payload_key: payload.get(payload_key, {}),
                "production_effect": "none",
                "broker_action": "none",
            },
        )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["source_feature_inventory_markdown"]),
        _inventory_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["pit_risk_audit_markdown"]),
        _pit_risk_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["remediation_plan_markdown"]),
        _remediation_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy growth tilt engine PIT signal remediation plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- input under review：`{payload.get('input_under_review')}`",
            f"- current severity：`{payload.get('current_severity')}`",
            f"- current PIT status：`{payload.get('current_pit_status')}`",
            (
                "- source feature inventory ready："
                f"`{payload.get('source_feature_inventory_ready')}`"
            ),
            f"- PIT risk audit ready：`{payload.get('pit_risk_audit_ready')}`",
            (
                "- signal construction gap analysis ready："
                f"`{payload.get('signal_construction_gap_analysis_ready')}`"
            ),
            f"- remediation plan ready：`{payload.get('remediation_plan_ready')}`",
            (
                "- severity downgrade conditions ready："
                f"`{payload.get('severity_downgrade_conditions_ready')}`"
            ),
            f"- validation plan ready：`{payload.get('validation_plan_ready')}`",
            (
                "- growth tilt blocking gap resolved："
                f"`{payload.get('growth_tilt_engine_blocking_gap_resolved')}`"
            ),
            (
                "- growth tilt severity downgraded："
                f"`{payload.get('growth_tilt_engine_severity_downgraded')}`"
            ),
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            (
                "- research-only observation allowed："
                f"`{payload.get('research_only_observation_allowed')}`"
            ),
            f"- paper-shadow allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production allowed：`{payload.get('production_allowed')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- data quality gate：not run；reason="
                f"`{payload.get('data_quality_gate_reason')}`"
            ),
            "",
            "## Source findings from TRADING-2405",
            "",
            f"- source validation errors：`{payload.get('source_validation_errors')}`",
            "",
            "## Current growth_tilt_engine blocker status",
            "",
            _json_block(payload.get("current_blocker", {})),
            "",
            "## Source feature inventory",
            "",
            _json_block(payload.get("source_feature_inventory", [])),
            "",
            "## PIT risk audit",
            "",
            _json_block(payload.get("pit_risk_audit", {})),
            "",
            "## Signal construction gap analysis",
            "",
            _json_block(payload.get("signal_construction_gap_analysis", {})),
            "",
            "## Remediation plan",
            "",
            _json_block(payload.get("remediation_plan", {})),
            "",
            "## Severity downgrade conditions",
            "",
            _json_block(payload.get("severity_downgrade_conditions", {})),
            "",
            "## Validation plan",
            "",
            _json_block(payload.get("validation_plan", {})),
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
        ]
    )


def _inventory_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy growth tilt engine source feature inventory",
            "",
            f"- status：`{payload.get('status')}`",
            f"- source feature count：`{payload.get('source_feature_inventory_count')}`",
            "",
            _json_block(payload.get("source_feature_inventory", [])),
            "",
        ]
    )


def _pit_risk_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy growth tilt engine PIT risk audit",
            "",
            f"- status：`{payload.get('status')}`",
            f"- risk count：`{payload.get('pit_risk_audit_count')}`",
            "",
            _json_block(payload.get("pit_risk_audit", {})),
            "",
        ]
    )


def _remediation_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy growth tilt engine remediation plan",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- blocking gap resolved："
                f"`{payload.get('growth_tilt_engine_blocking_gap_resolved')}`"
            ),
            (
                "- severity downgraded："
                f"`{payload.get('growth_tilt_engine_severity_downgraded')}`"
            ),
            "",
            _json_block(payload.get("remediation_plan", {})),
            "",
            "## Severity downgrade conditions",
            "",
            _json_block(payload.get("severity_downgrade_conditions", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2407 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            f"- reason：`{payload.get('recommended_next_research_task_reason')}`",
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            (
                "- research-only observation allowed："
                f"`{payload.get('research_only_observation_allowed')}`"
            ),
            f"- paper-shadow allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production allowed：`{payload.get('production_allowed')}`",
            f"- broker action enabled：`{payload.get('broker_action_enabled')}`",
            "",
        ]
    )


def _pit_row(sources: Mapping[str, Any], input_id: str) -> Mapping[str, Any]:
    for source_key in ("pit_matrix_2405", "pit_matrix_2403"):
        source = _as_mapping(sources.get(source_key))
        rows = _as_list(
            _as_mapping(source.get("pit_coverage_matrix")).get("pit_coverage_matrix")
        )
        if not rows:
            rows = _as_list(source.get("pit_coverage_matrix"))
        for row in (_as_mapping(item) for item in rows):
            if row.get("input_id") == input_id:
                return row
    return {}


def _growth_signal_review(sources: Mapping[str, Any]) -> Mapping[str, Any]:
    review = _as_mapping(
        _as_mapping(sources.get("signal_construction_review_2403")).get(
            "signal_construction_review"
        )
    )
    return _as_mapping(review.get(INPUT_UNDER_REVIEW))


def _registry_entry(registry: Mapping[str, Any], input_id: str) -> Mapping[str, Any]:
    entries = _as_list(registry.get("entries"))
    for entry in (_as_mapping(item) for item in entries):
        if entry.get("input_id") == input_id:
            return entry
    return {}


def _growth_tilt_candidate_family(config: Mapping[str, Any]) -> Mapping[str, Any]:
    for family in (_as_mapping(item) for item in _as_list(config.get("candidate_families"))):
        if family.get("candidate_family") == "vol_target_growth_tilt":
            return family
    return {}


def _execution_policy_for_focused_candidate(
    registry: Mapping[str, Any],
) -> Mapping[str, Any]:
    for policy in (
        _as_mapping(item)
        for item in _as_list(registry.get("strategy_execution_policies"))
    ):
        if policy.get("strategy_id") == FOCUSED_GROWTH_TILT_CANDIDATE_ID:
            return policy
    return {}


def _join_list(value: Any, default: str) -> str:
    values = _as_list(value)
    if not values:
        return default
    return ",".join(str(item) for item in values)


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
