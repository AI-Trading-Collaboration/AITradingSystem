from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_execution_cadence_bias_audit as m2364
import ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest as m2386
import ai_trading_system.dynamic_strategy_recombination_line_plateau_decision as m2401
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
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY

TASK_ID = "TRADING-2402"
TASK_REGISTER_ID = (
    "TRADING-2402_DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW"
)
REPORT_TYPE = "dynamic_strategy_data_pit_signal_quality_gap_review"
SCHEMA_VERSION = "dynamic_strategy_data_pit_and_signal_quality_gap_review.v1"
READY_STATUS = "DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_BLOCKED_SOURCE_ARTIFACT"
)
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_BLOCKED_DATA_QUALITY"
)
NEXT_ROUTE = (
    "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_"
    "Review"
)
DEFAULT_DATA_QUALITY_AS_OF = date(2026, 7, 5)
VALIDATE_DATA_AUDIT_DIR = PROJECT_ROOT / "artifacts" / "data_refresh_audit" / "validation"

SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2364",
    "TRADING-2386",
    "TRADING-2399",
    "TRADING-2400",
    "TRADING-2401",
)
DEFAULT_RECOMMENDED_OPTIONS: tuple[str, ...] = (
    "OPTION_B_BUILD_PIT_COVERAGE_MATRIX",
    "OPTION_C_REVIEW_SIGNAL_CONSTRUCTION_FRAMEWORK",
    "OPTION_E_BUILD_THRESHOLD_META_DATASET",
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
    "new_strategy_backtest",
    "new_trading_signal",
    "new_scoring",
    "resume_candidate_search",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "observation_approved",
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

DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH = (
    m2401.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT
    / "plateau_decision_result.json"
)
DEFAULT_SOURCE_2401_ROUTE_PATH = (
    m2401.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT
    / "data_signal_quality_review_route.json"
)
DEFAULT_SOURCE_2400_OWNER_REVIEW_PATH = (
    m2400.DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2399_TARGETED_RETEST_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "targeted_gate_evidence_retest_result.json"
)
DEFAULT_SOURCE_2399_GATE_EVIDENCE_MATRIX_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "gate_evidence_matrix.json"
)
DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2386_EXPANDED_RETEST_PATH = (
    m2386.DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2386_SIGNAL_FAMILY_SCREENING_PATH = (
    m2386.DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "signal_family_screening.json"
)
DEFAULT_SOURCE_2364_CADENCE_BIAS_AUDIT_PATH = (
    m2364.DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT
    / "execution_cadence_bias_audit.json"
)


def run_dynamic_strategy_data_pit_signal_quality_gap_review(
    *,
    source_plateau_decision_2401_path: Path = DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH,
    source_route_2401_path: Path = DEFAULT_SOURCE_2401_ROUTE_PATH,
    source_owner_review_2400_path: Path = DEFAULT_SOURCE_2400_OWNER_REVIEW_PATH,
    source_targeted_retest_2399_path: Path = DEFAULT_SOURCE_2399_TARGETED_RETEST_PATH,
    source_gate_evidence_matrix_2399_path: Path = (
        DEFAULT_SOURCE_2399_GATE_EVIDENCE_MATRIX_PATH
    ),
    source_decision_update_2399_path: Path = DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH,
    source_expanded_retest_2386_path: Path = DEFAULT_SOURCE_2386_EXPANDED_RETEST_PATH,
    source_signal_family_screening_2386_path: Path = (
        DEFAULT_SOURCE_2386_SIGNAL_FAMILY_SCREENING_PATH
    ),
    source_cadence_bias_audit_2364_path: Path = (
        DEFAULT_SOURCE_2364_CADENCE_BIAS_AUDIT_PATH
    ),
    source_validate_data_audit_path: Path | None = None,
    source_validate_data_report_path: Path | None = None,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    ),
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_DOCS_ROOT,
    as_of_date: date | None = None,
    validate_data_as_of: date = DEFAULT_DATA_QUALITY_AS_OF,
) -> dict[str, Any]:
    sources = _load_sources(
        source_plateau_decision_2401_path=source_plateau_decision_2401_path,
        source_route_2401_path=source_route_2401_path,
        source_owner_review_2400_path=source_owner_review_2400_path,
        source_targeted_retest_2399_path=source_targeted_retest_2399_path,
        source_gate_evidence_matrix_2399_path=source_gate_evidence_matrix_2399_path,
        source_decision_update_2399_path=source_decision_update_2399_path,
        source_expanded_retest_2386_path=source_expanded_retest_2386_path,
        source_signal_family_screening_2386_path=(
            source_signal_family_screening_2386_path
        ),
        source_cadence_bias_audit_2364_path=source_cadence_bias_audit_2364_path,
        source_validate_data_audit_path=source_validate_data_audit_path,
        source_validate_data_report_path=source_validate_data_report_path,
        validate_data_as_of=validate_data_as_of,
    )
    validation_errors = _validate_sources(sources)
    data_quality_errors = _validate_data_quality_gate(sources["validate_data_audit"])
    ready = not validation_errors and not data_quality_errors
    payload = _base_payload(
        status=(
            READY_STATUS
            if ready
            else BLOCKED_DATA_QUALITY_STATUS
            if data_quality_errors
            else BLOCKED_SOURCE_STATUS
        ),
        sources=sources,
        as_of_date=as_of_date,
        validate_data_as_of=validate_data_as_of,
        source_validation_errors=validation_errors,
        data_quality_validation_errors=data_quality_errors,
    )
    if ready:
        payload.update(_ready_sections(sources))
    else:
        payload.update(_blocked_sections())
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_plateau_decision_2401_path: Path,
    source_route_2401_path: Path,
    source_owner_review_2400_path: Path,
    source_targeted_retest_2399_path: Path,
    source_gate_evidence_matrix_2399_path: Path,
    source_decision_update_2399_path: Path,
    source_expanded_retest_2386_path: Path,
    source_signal_family_screening_2386_path: Path,
    source_cadence_bias_audit_2364_path: Path,
    source_validate_data_audit_path: Path | None,
    source_validate_data_report_path: Path | None,
    validate_data_as_of: date,
) -> dict[str, Any]:
    audit_path = source_validate_data_audit_path or _latest_validate_data_audit_path(
        validate_data_as_of
    )
    report_path = source_validate_data_report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"data_quality_{validate_data_as_of}.md"
    )
    sources = {
        "plateau_decision_2401": _load_json_document(source_plateau_decision_2401_path),
        "route_2401": _load_json_document(source_route_2401_path),
        "owner_review_2400": _load_json_document(source_owner_review_2400_path),
        "targeted_retest_2399": _load_json_document(source_targeted_retest_2399_path),
        "gate_evidence_matrix_2399": _load_json_document(
            source_gate_evidence_matrix_2399_path
        ),
        "decision_update_2399": _load_json_document(source_decision_update_2399_path),
        "expanded_retest_2386": _load_json_document(source_expanded_retest_2386_path),
        "signal_family_screening_2386": _load_json_document(
            source_signal_family_screening_2386_path
        ),
        "cadence_bias_audit_2364": _load_json_document(
            source_cadence_bias_audit_2364_path
        ),
        "validate_data_audit": (
            {"_missing": True, "_path": ""}
            if audit_path is None
            else _load_json_document(audit_path)
        ),
        "validate_data_report_issues": _parse_data_quality_report(report_path),
    }
    sources["source_paths"] = {
        "plateau_decision_2401": str(source_plateau_decision_2401_path),
        "route_2401": str(source_route_2401_path),
        "owner_review_2400": str(source_owner_review_2400_path),
        "targeted_retest_2399": str(source_targeted_retest_2399_path),
        "gate_evidence_matrix_2399": str(source_gate_evidence_matrix_2399_path),
        "decision_update_2399": str(source_decision_update_2399_path),
        "expanded_retest_2386": str(source_expanded_retest_2386_path),
        "signal_family_screening_2386": str(source_signal_family_screening_2386_path),
        "cadence_bias_audit_2364": str(source_cadence_bias_audit_2364_path),
        "validate_data_audit": "" if audit_path is None else str(audit_path),
        "validate_data_report": str(report_path),
    }
    return sources


def _latest_validate_data_audit_path(as_of: date) -> Path | None:
    pattern = f"validate_data_{as_of.isoformat()}_*.json"
    candidates = sorted(
        VALIDATE_DATA_AUDIT_DIR.glob(pattern),
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    return candidates[-1] if candidates else None


def _parse_data_quality_report(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return [{"severity": "MISSING", "code": "data_quality_report_missing"}]
    rows: list[dict[str, Any]] = []
    in_table = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("| 级别 | 来源 | Code |"):
            in_table = True
            continue
        if not in_table:
            continue
        if line.startswith("|---"):
            continue
        if not line.startswith("|"):
            break
        cells = [cell.strip().strip("`") for cell in line.strip().strip("|").split("|")]
        if len(cells) < 6:
            continue
        rows.append(
            {
                "severity": cells[0],
                "source": cells[1],
                "code": cells[2],
                "row_count": cells[3],
                "description": cells[4],
                "sample": cells[5],
            }
        )
    return rows


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_statuses = {
        "plateau_decision_2401": m2401.READY_STATUS,
        "route_2401": m2401.READY_STATUS,
        "owner_review_2400": m2400.READY_STATUS,
        "targeted_retest_2399": m2399.READY_STATUS,
        "gate_evidence_matrix_2399": m2399.READY_STATUS,
        "decision_update_2399": m2399.READY_STATUS,
        "expanded_retest_2386": m2386.READY_STATUS,
        "signal_family_screening_2386": m2386.READY_STATUS,
        "cadence_bias_audit_2364": m2364.READY_STATUS,
    }
    for source_name, expected in expected_statuses.items():
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing source artifact {source.get('_path')}")
        elif source.get("status") != expected:
            errors.append(
                f"{source_name}: expected status {expected}, observed {source.get('status')}"
            )
    plateau = _as_mapping(sources.get("plateau_decision_2401"))
    route_2401 = _as_mapping(sources.get("route_2401"))
    route = _as_mapping(route_2401.get("data_signal_quality_review_route"))
    owner_2400 = _as_mapping(sources.get("owner_review_2400"))
    retest_2399 = _as_mapping(sources.get("targeted_retest_2399"))
    decision_2399 = _as_mapping(
        _as_mapping(sources.get("decision_update_2399")).get("decision_update")
    )
    if plateau.get("owner_decision") != m2401.OWNER_DECISION:
        errors.append("2401 owner decision mismatch")
    if plateau.get("recombination_line_plateau_detected") is not True:
        errors.append("2401 plateau was not detected")
    if plateau.get("continue_local_targeted_improvement_recommended") is not False:
        errors.append("2401 did not pause local targeted improvement")
    if plateau.get("recommended_next_research_task") != m2401.NEXT_ROUTE:
        errors.append("2401 next route mismatch")
    if route.get("recommended_next_research_task") != m2401.NEXT_ROUTE:
        errors.append("2401 route artifact mismatch")
    if owner_2400.get("owner_decision") != m2400.OWNER_DECISION:
        errors.append("2400 owner decision mismatch")
    if retest_2399.get("best_targeted_variant") != m2401.BEST_TARGETED_VARIANT:
        errors.append("2399 best targeted variant mismatch")
    if retest_2399.get("observation_preview_candidates_count") != 0:
        errors.append("2399 observation preview count mismatch")
    if decision_2399.get("research_only_observation_preview_exists") is True:
        errors.append("2399 unexpectedly found observation preview candidate")
    _validate_source_safety(sources, errors)
    return errors


def _validate_source_safety(sources: Mapping[str, Any], errors: list[str]) -> None:
    for source_name in (
        "plateau_decision_2401",
        "route_2401",
        "owner_review_2400",
        "targeted_retest_2399",
        "decision_update_2399",
        "expanded_retest_2386",
        "cadence_bias_audit_2364",
    ):
        source = _as_mapping(sources.get(source_name))
        for field in SAFETY_FALSE_FIELDS:
            if source.get(field) is True:
                errors.append(f"{source_name}: safety field must be false: {field}")
        if source.get("broker_action") not in (None, "none"):
            errors.append(f"{source_name}: broker_action must be none")


def _validate_data_quality_gate(validate_audit: Mapping[str, Any]) -> list[str]:
    if validate_audit.get("_missing"):
        return ["validate-data audit artifact is missing"]
    status = _validate_data_status(validate_audit)
    errors = _int_value(validate_audit.get("error_count"))
    if status not in {"PASS", "PASS_WITH_WARNINGS"}:
        return [f"validate-data status is not passing: {status}"]
    if errors != 0:
        return [f"validate-data error_count must be 0: {errors}"]
    return []


def _base_payload(
    *,
    status: str,
    sources: Mapping[str, Any],
    as_of_date: date | None,
    validate_data_as_of: date,
    source_validation_errors: list[str],
    data_quality_validation_errors: list[str],
) -> dict[str, Any]:
    validate_audit = _as_mapping(sources.get("validate_data_audit"))
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
        "data_quality_validation_errors": data_quality_validation_errors,
        "validate_data_as_of": validate_data_as_of.isoformat(),
        "validate_data_status": _validate_data_status(validate_audit),
        "validate_data_error_count": _int_value(validate_audit.get("error_count")),
        "validate_data_warning_count": _int_value(validate_audit.get("warning_count")),
        "validate_data_info_count": _int_value(validate_audit.get("info_count")),
        "validate_data_report_path": validate_audit.get("report_path"),
        "data_quality_gate_executed": True,
        "data_quality_gate_command": f"aits validate-data --as-of {validate_data_as_of}",
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "fresh_market_data_read_by_2402": False,
        "owner_decision_from_2401": m2401.OWNER_DECISION,
        "recombination_line_paused": True,
        "manual_review_required": True,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "observation_approved": False,
        "paper_shadow_allowed": False,
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
        "production_allowed": False,
        "production_approved": False,
        "production_enabled": False,
        "broker_action": "none",
        "broker_action_enabled": False,
        "order_generated": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    data_quality_review = _data_quality_review(sources)
    pit_review = _pit_coverage_review(sources)
    signal_review = _signal_quality_review(sources)
    regime_review = _regime_labeling_review(sources)
    threshold_review = _threshold_meta_dataset_review(sources)
    gap_matrix = _prioritized_gap_matrix(
        data_quality_review=data_quality_review,
        pit_review=pit_review,
        signal_review=signal_review,
        regime_review=regime_review,
        threshold_review=threshold_review,
    )
    next_decision = _next_research_direction_decision()
    return {
        "data_quality_gap_review_ready": True,
        "pit_coverage_gap_review_ready": True,
        "signal_quality_gap_review_ready": True,
        "regime_labeling_gap_review_ready": True,
        "threshold_meta_dataset_gap_review_ready": True,
        "prioritized_gap_matrix_ready": True,
        "data_quality_gap_review": data_quality_review,
        "pit_coverage_gap_review": pit_review,
        "signal_quality_gap_review": signal_review,
        "regime_labeling_gap_review": regime_review,
        "threshold_meta_dataset_gap_review": threshold_review,
        "prioritized_gap_matrix": gap_matrix,
        "next_research_direction_decision": next_decision,
        "recommended_next_research_task": NEXT_ROUTE,
        "resume_candidate_search_recommended": False,
        "pit_coverage_matrix_recommended": True,
        "signal_construction_review_recommended": True,
        "regime_expectation_scoring_review_recommended": True,
        "threshold_meta_dataset_recommended": True,
        "data_quality_warning_classification_required": True,
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "data_quality_gap_review_ready": False,
        "pit_coverage_gap_review_ready": False,
        "signal_quality_gap_review_ready": False,
        "regime_labeling_gap_review_ready": False,
        "threshold_meta_dataset_gap_review_ready": False,
        "prioritized_gap_matrix_ready": False,
        "data_quality_gap_review": {},
        "pit_coverage_gap_review": {},
        "signal_quality_gap_review": {},
        "regime_labeling_gap_review": {},
        "threshold_meta_dataset_gap_review": {},
        "prioritized_gap_matrix": [],
        "next_research_direction_decision": {},
        "recommended_next_research_task": None,
        "resume_candidate_search_recommended": False,
        "pit_coverage_matrix_recommended": False,
        "signal_construction_review_recommended": False,
        "regime_expectation_scoring_review_recommended": False,
        "threshold_meta_dataset_recommended": False,
    }


def _data_quality_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    audit = _as_mapping(sources.get("validate_data_audit"))
    issues = _as_list(sources.get("validate_data_report_issues"))
    warning_rows = [row for row in issues if _text(row.get("severity")) == "警告"]
    classified = [_classify_data_quality_issue(row) for row in warning_rows]
    file_summaries = _as_mapping(audit.get("file_summaries"))
    price = _as_mapping(file_summaries.get("price_data"))
    secondary = _as_mapping(file_summaries.get("secondary_price_data"))
    rates = _as_mapping(file_summaries.get("macro_rate_data"))
    relevant = [
        row for row in classified if row.get("dynamic_strategy_relevance") != "LOW"
    ]
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_data_quality_gap_review.v1",
        "latest_validate_data_status": _validate_data_status(audit),
        "error_count": _int_value(audit.get("error_count")),
        "warning_count": _int_value(audit.get("warning_count")),
        "info_count": _int_value(audit.get("info_count")),
        "cached_market_data": {
            "coverage_start": price.get("min_date"),
            "coverage_end": price.get("max_date"),
            "price_row_count": price.get("rows"),
            "secondary_coverage_start": secondary.get("min_date"),
            "secondary_coverage_end": secondary.get("max_date"),
            "secondary_price_row_count": secondary.get("rows"),
            "macro_coverage_start": rates.get("min_date"),
            "macro_coverage_end": rates.get("max_date"),
            "macro_rate_row_count": rates.get("rows"),
            "missing_date_count": "NOT_EXPOSED_BY_VALIDATE_DATA_AUDIT",
            "missing_symbol_count": "NOT_EXPOSED_BY_VALIDATE_DATA_AUDIT",
            "stale_data_risk": (
                "MINOR_WEEKEND_OR_HOLIDAY_AS_OF_WITH_LAST_PRICE_"
                f"{price.get('max_date')}"
            ),
            "split_dividend_adjustment_risk": (
                "MATERIAL_TQQQ_ADJUSTMENT_RATIO_WARNING"
                if any(row.get("code") == "prices_adjustment_ratio_jump" for row in classified)
                else "NO_WARNING"
            ),
            "corporate_action_handling": (
                "known split events are recorded; one TQQQ adjustment-ratio "
                "warning remains reviewable"
            ),
        },
        "warning_detail_summary": classified,
        "warnings_relevant_to_dynamic_strategy": relevant,
        "warnings_irrelevant_to_dynamic_strategy": [
            row for row in classified if row.get("dynamic_strategy_relevance") == "LOW"
        ],
        "pass_with_warnings_interpretation": (
            "PASS_WITH_WARNINGS 不阻断 2402 review；但 TQQQ adjustment-ratio warning "
            "直接触及 dynamic strategy universe，后续候选解释必须保留 caveat。"
        ),
    }


def _classify_data_quality_issue(row: Mapping[str, Any]) -> dict[str, Any]:
    code = _text(row.get("code"))
    if code == "prices_download_manifest_checksum_missing":
        return {
            **dict(row),
            "gap_category": "DATA_QUALITY",
            "dynamic_strategy_relevance": "MATERIAL",
            "likely_impact": (
                "cache provenance is incomplete; ranking math not directly changed "
                "but auditability is weakened"
            ),
            "recommended_fix": (
                "reconcile price cache checksum with download_manifest or rerun "
                "audited download-data path"
            ),
        }
    if code == "prices_adjustment_ratio_jump":
        return {
            **dict(row),
            "gap_category": "DATA_QUALITY",
            "dynamic_strategy_relevance": "MATERIAL",
            "likely_impact": (
                "TQQQ is part of the dynamic strategy universe; unresolved "
                "adjustment-ratio warning can affect leveraged exposure interpretation"
            ),
            "recommended_fix": (
                "investigate TQQQ corporate-action / adjusted-close ratio and "
                "document whether it is vendor basis or cache error"
            ),
        }
    return {
        **dict(row),
        "gap_category": "DATA_QUALITY",
        "dynamic_strategy_relevance": "LOW",
        "likely_impact": "not currently identified as dynamic-strategy ranking blocker",
        "recommended_fix": "keep disclosed in data quality report",
    }


def _pit_coverage_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    plateau = _as_mapping(sources.get("plateau_decision_2401"))
    route = _as_mapping(plateau.get("data_signal_quality_review_scope"))
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_pit_coverage_gap_review.v1",
        "feature_pit_status": {
            "which_features_are_point_in_time": [
                "cached QQQ/TQQQ/SGOV prices are historical rows validated by as_of gate",
                "rates_daily.csv has source checksums and date range in validate-data audit",
            ],
            "which_features_are_approximate_pit": [
                "dynamic strategy regime labels derived from historical price behavior",
                (
                    "signal valid-until window derived from research policy rather "
                    "than observed signal expiry distribution"
                ),
            ],
            "which_features_are_not_pit_safe": [],
            "which_features_depend_on_later_data": [],
        },
        "signal_pit_status": {
            "signal_generation_as_of_date_correctness": "MATERIAL_REVIEW_REQUIRED",
            "advisory_valid_from_correctness": "MATERIAL_REVIEW_REQUIRED",
            "advisory_valid_until_correctness": "MATERIAL_REVIEW_REQUIRED",
            "signal_horizon_definition": "MATERIAL_REVIEW_REQUIRED",
            "revision_or_restated_data_risk": (
                "LOW_FOR_PRICE_ROWS_BUT_NOT_FULLY_AUDITED_FOR_SIGNAL_DERIVED_FEATURES"
            ),
        },
        "outcome_binding_status": {
            "outcome_binding_disabled": True,
            "no_mutation_confirmed": True,
            "future_outcome_dependency_risk": (
                "NO_MUTATION_IN_2402_BUT_PIT_MATRIX_REQUIRED_BEFORE_OBSERVATION"
            ),
        },
        "pit_gap_severity": {
            "feature_signal_pit_matrix_missing": "MATERIAL",
            "valid_until_pit_lineage_missing": "MATERIAL",
            "outcome_binding": "NOT_APPLICABLE",
        },
        "source_scope_from_2401": route.get("PIT_coverage", []),
    }


def _signal_quality_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    targeted = _as_mapping(sources.get("targeted_retest_2399"))
    gate_matrix = _as_list(
        _as_mapping(sources.get("gate_evidence_matrix_2399")).get("gate_evidence_matrix")
    )
    best_row = _best_candidate_gate_row(gate_matrix, m2401.BEST_TARGETED_VARIANT)
    signal_family = _as_list(
        _as_mapping(sources.get("signal_family_screening_2386")).get(
            "signal_family_screening"
        )
    )
    best_signal_family = signal_family[0] if signal_family else {}
    valid_until = _as_mapping(best_row.get("valid_until_stale_signal_evidence"))
    execution_metrics = _as_mapping(best_row.get("execution_metrics"))
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_signal_quality_gap_review.v1",
        "candidate_plateau_interpretation": (
            "更可能是 signal / PIT / regime / threshold evidence 质量限制，而不是单纯缺少"
            "局部 recombination variants。"
        ),
        "growth_tilt_engine": {
            "source_features": ["growth_tilt_engine", "guarded_turnover_transfer"],
            "signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
            "signal_confidence_if_available": "NOT_EXPOSED",
            "signal_decay_rule": "APPROXIMATE_VALID_UNTIL_STRICTNESS",
            "valid_until_rule": "validity_10d_v1 / valid_until_window family",
            "historical_stability": "MATERIAL_REVIEW_REQUIRED",
            "false_positive_risk": "MATERIAL_REGIME_AND_DRAWDOWN_FAILURE_RISK",
            "false_negative_risk": "MATERIAL_MISSED_SIGNAL_COUNT_NONZERO",
        },
        "lower_turnover_guardrail": {
            "turnover_budget_source": "research policy / targeted variant construction",
            "cooldown_rule_source": "min_holding and cooldown policy from prior retests",
            "max_step_delta_source": "targeted variant pilot constants",
            "effect_on_cost_adjusted_return": "cost stress often survives but return gap remains",
            "effect_on_return_gap": "guardrail can preserve cost but may cap growth tilt upside",
        },
        "valid_until_strictness": {
            "stale_signal_execution_count": valid_until.get(
                "stale_signal_execution_count",
                execution_metrics.get("stale_signal_execution_count"),
            ),
            "near_expiry_signal_behavior": "NOT_SEPARATELY_VALIDATED",
            "signal_to_execution_lag_days": valid_until.get(
                "signal_to_execution_lag_days",
                execution_metrics.get("signal_to_execution_lag_days"),
            ),
            "strict_expiry_tradeoff": (
                "strict expiry removes stale carry but may increase missed signals "
                "or turnover tradeoff"
            ),
        },
        "best_targeted_variant": targeted.get("best_targeted_variant"),
        "best_targeted_variant_decision": targeted.get("best_targeted_variant_decision"),
        "observation_preview_candidates_count": targeted.get(
            "observation_preview_candidates_count"
        ),
        "best_signal_family_from_2386": best_signal_family,
    }


def _regime_labeling_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    gate_matrix = _as_list(
        _as_mapping(sources.get("gate_evidence_matrix_2399")).get("gate_evidence_matrix")
    )
    best_row = _best_candidate_gate_row(gate_matrix, m2401.BEST_TARGETED_VARIANT)
    regime = _as_mapping(best_row.get("regime_expectation_evidence"))
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_regime_labeling_gap_review.v1",
        "current_regime_labels": [
            "risk_on",
            "risk_off",
            "high_volatility",
            "low_volatility",
            "trend_confirmed",
            "recovery",
        ],
        "review_questions": {
            "labels_from_explicit_rules": "PARTIAL_PRIOR_ARTIFACT_RULES_EXIST_BUT_NOT_NORMALIZED",
            "lookahead_risk": "MATERIAL_REVIEW_REQUIRED",
            "labels_too_coarse": True,
            "aligned_with_strategy_expected_behavior": "NOT_YET_CALIBRATED",
            "replace_regime_slice_pass_rate": "RECOMMEND_REGIME_EXPECTATION_SCORE",
            "should_growth_tilt_outperform_static_in_all_regimes": False,
        },
        "regime_expectation_score_from_best_variant": regime.get(
            "regime_expectation_score"
        ),
        "regime_expectation_not_weak": regime.get("regime_expectation_not_weak"),
        "regime_expectation_policy_needed": True,
        "reason": (
            "不同策略在不同 regime 的预期行为不同，不能用统一 pass/fail 标准评估所有"
            "regime；应转为 strategy-specific expectation score。"
        ),
    }


def _threshold_meta_dataset_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    decision_2399 = _as_mapping(
        _as_mapping(sources.get("decision_update_2399")).get("decision_update")
    )
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_threshold_meta_dataset_gap_review.v1",
        "needed_for": [
            "time_slice_pass_rate_threshold",
            "regime_expectation_score_threshold",
            "drawdown_materiality_threshold",
            "return_per_drawdown_penalty_threshold",
            "owner_review_required_vs_continue_optimization_boundary",
        ],
        "current_status": {
            "no_full_meta_dataset_yet": True,
            "candidate_results_exist_across_many_tasks": True,
            "results_need_normalization_into_matrix": True,
            "current_2399_candidate_decisions": decision_2399.get("candidate_decisions"),
        },
        "proposed_matrix_dimensions": [
            "candidate_id",
            "source_task",
            "execution_cadence",
            "cost_stress",
            "time_slice_pass_rate",
            "regime_slice_or_expectation_score",
            "dynamic_vs_static_gap",
            "drawdown_gap",
            "turnover",
            "decision",
            "subsequent_reclassification",
        ],
        "build_before_more_candidate_search": True,
    }


def _prioritized_gap_matrix(
    *,
    data_quality_review: Mapping[str, Any],
    pit_review: Mapping[str, Any],
    signal_review: Mapping[str, Any],
    regime_review: Mapping[str, Any],
    threshold_review: Mapping[str, Any],
) -> list[dict[str, Any]]:
    warnings = _as_list(data_quality_review.get("warnings_relevant_to_dynamic_strategy"))
    data_gaps = [
        {
            "gap_id": f"2402-DATA-{index:02d}",
            "gap_category": "DATA_QUALITY",
            "gap_description": _text(row.get("description")),
            "severity": _text(row.get("dynamic_strategy_relevance"), "MATERIAL"),
            "affected_research_tasks": ["TRADING-2364", "TRADING-2386", "TRADING-2399"],
            "affected_candidates": ["QQQ", "TQQQ", "SGOV"],
            "likely_impact": row.get("likely_impact"),
            "recommended_fix": row.get("recommended_fix"),
            "recommended_next_task": NEXT_ROUTE,
            "owner_review_required": True,
        }
        for index, row in enumerate(warnings, start=1)
    ]
    structural = [
        {
            "gap_id": "2402-PIT-01",
            "gap_category": "PIT_COVERAGE",
            "gap_description": (
                "feature / signal / advisory valid-from / valid-until PIT coverage "
                "matrix is missing"
            ),
            "severity": _as_mapping(pit_review.get("pit_gap_severity")).get(
                "feature_signal_pit_matrix_missing", "MATERIAL"
            ),
            "affected_research_tasks": list(SOURCE_TASKS),
            "affected_candidates": ["dynamic strategy candidate family"],
            "likely_impact": (
                "candidate ranking cannot be promoted to observation without "
                "explicit PIT lineage"
            ),
            "recommended_fix": (
                "build PIT coverage matrix for feature, signal, advisory and "
                "outcome fields"
            ),
            "recommended_next_task": NEXT_ROUTE,
            "owner_review_required": True,
        },
        {
            "gap_id": "2402-SIGNAL-01",
            "gap_category": "SIGNAL_QUALITY",
            "gap_description": (
                "growth_tilt / valid_until signal quality is not separately "
                "validated from portfolio-combination effects"
            ),
            "severity": "MATERIAL",
            "affected_research_tasks": ["TRADING-2386", "TRADING-2399", "TRADING-2401"],
            "affected_candidates": [m2401.BASE_CANDIDATE, m2401.BEST_TARGETED_VARIANT],
            "likely_impact": signal_review.get("candidate_plateau_interpretation"),
            "recommended_fix": (
                "review signal construction framework and isolate signal stability "
                "/ false-positive / false-negative behavior"
            ),
            "recommended_next_task": NEXT_ROUTE,
            "owner_review_required": True,
        },
        {
            "gap_id": "2402-VALIDUNTIL-01",
            "gap_category": "VALID_UNTIL_AND_STALE_SIGNAL",
            "gap_description": (
                "valid-until window is still a research policy approximation rather "
                "than calibrated signal expiry evidence"
            ),
            "severity": "MATERIAL",
            "affected_research_tasks": ["TRADING-2364", "TRADING-2386", "TRADING-2399"],
            "affected_candidates": [m2401.BEST_TARGETED_VARIANT],
            "likely_impact": "stale-signal and missed-signal tradeoff can change candidate ranking",
            "recommended_fix": (
                "build signal-age / expiry evidence before further valid-until "
                "variants"
            ),
            "recommended_next_task": NEXT_ROUTE,
            "owner_review_required": True,
        },
        {
            "gap_id": "2402-REGIME-01",
            "gap_category": "REGIME_LABELING",
            "gap_description": (
                "regime labels are too coarse for strategy-specific expected behavior"
            ),
            "severity": "MATERIAL",
            "affected_research_tasks": ["TRADING-2386", "TRADING-2399"],
            "affected_candidates": ["growth_tilt strategy family"],
            "likely_impact": regime_review.get("reason"),
            "recommended_fix": "replace raw regime pass-rate with regime expectation score policy",
            "recommended_next_task": NEXT_ROUTE,
            "owner_review_required": True,
        },
        {
            "gap_id": "2402-THRESHOLD-01",
            "gap_category": "THRESHOLD_CALIBRATION",
            "gap_description": "candidate gate thresholds lack normalized historical meta-dataset",
            "severity": "MATERIAL",
            "affected_research_tasks": list(SOURCE_TASKS),
            "affected_candidates": ["all dynamic strategy candidates"],
            "likely_impact": "owner-review vs continue-optimization boundary remains subjective",
            "recommended_fix": (
                "normalize candidate x threshold x decision history into a "
                "calibration matrix"
            ),
            "recommended_next_task": NEXT_ROUTE,
            "owner_review_required": True,
        },
        {
            "gap_id": "2402-REPORTING-01",
            "gap_category": "REPORTING_AND_ARTIFACT_NORMALIZATION",
            "gap_description": (
                "historical candidate evidence exists but is distributed across "
                "task-specific schemas"
            ),
            "severity": "MINOR",
            "affected_research_tasks": list(SOURCE_TASKS),
            "affected_candidates": ["all dynamic strategy candidates"],
            "likely_impact": (
                "owner review is slower and threshold calibration remains harder "
                "to audit"
            ),
            "recommended_fix": "define normalized dynamic strategy candidate evidence matrix",
            "recommended_next_task": NEXT_ROUTE,
            "owner_review_required": False,
        },
    ]
    return [*data_gaps, *structural]


def _next_research_direction_decision() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_options": list(DEFAULT_RECOMMENDED_OPTIONS),
        "recommended_priority": {
            "P0": [
                "PIT coverage matrix",
                "signal construction quality review",
                "regime labeling expectation review",
            ],
            "P1": [
                "threshold meta-dataset",
                "data quality warning classification",
            ],
            "P2": ["resume candidate search"],
        },
        "decision_options": {
            "OPTION_A_FIX_DATA_QUALITY_WARNINGS_FIRST": {
                "meaning": "先处理 validate-data warnings 和 cached data coverage gap",
                "recommended": False,
            },
            "OPTION_B_BUILD_PIT_COVERAGE_MATRIX": {
                "meaning": "先把 feature / signal / advisory 的 PIT 安全性系统化",
                "recommended": True,
            },
            "OPTION_C_REVIEW_SIGNAL_CONSTRUCTION_FRAMEWORK": {
                "meaning": (
                    "回到 signal 本身，审计 growth_tilt / valid_until / "
                    "regime signal quality"
                ),
                "recommended": True,
            },
            "OPTION_D_REBUILD_REGIME_EXPECTATION_SCORING": {
                "meaning": "替换粗糙 regime pass-rate，改成 regime expectation score",
                "recommended": True,
            },
            "OPTION_E_BUILD_THRESHOLD_META_DATASET": {
                "meaning": "汇总历史 candidate x threshold x decision，校准 gate",
                "recommended": True,
            },
            "OPTION_F_RESUME_STRATEGY_CANDIDATE_SEARCH": {
                "meaning": "继续候选策略搜索",
                "recommended": False,
            },
        },
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _best_candidate_gate_row(rows: list[Any], candidate_id: str) -> dict[str, Any]:
    for row in rows:
        mapped = _as_mapping(row)
        if mapped.get("candidate_id") == candidate_id:
            return dict(mapped)
    return {}


def _validate_data_status(audit: Mapping[str, Any]) -> str:
    return _text(
        _as_mapping(audit.get("quality_gate")).get("data_quality_status")
        or audit.get("raw_status")
        or audit.get("status")
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "gap_review_result.json"),
        "data_quality_gap_matrix_json": str(output_root / "data_quality_gap_matrix.json"),
        "pit_coverage_gap_review_json": str(output_root / "pit_coverage_gap_review.json"),
        "signal_quality_gap_review_json": str(output_root / "signal_quality_gap_review.json"),
        "regime_labeling_gap_review_json": str(
            output_root / "regime_labeling_gap_review.json"
        ),
        "threshold_meta_dataset_gap_review_json": str(
            output_root / "threshold_meta_dataset_gap_review.json"
        ),
        "markdown_path": str(docs_root / "dynamic_strategy_data_pit_signal_quality_gap_review.md"),
        "data_quality_gap_matrix_markdown": str(
            docs_root / "dynamic_strategy_data_quality_gap_matrix.md"
        ),
        "pit_coverage_gap_review_markdown": str(
            docs_root / "dynamic_strategy_pit_coverage_gap_review.md"
        ),
        "signal_quality_gap_review_markdown": str(
            docs_root / "dynamic_strategy_signal_quality_gap_review.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2403_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["data_quality_gap_matrix_json"]),
        {
            "task_id": TASK_ID,
            "status": payload.get("status"),
            "report_type": "dynamic_strategy_data_quality_gap_matrix",
            "schema_version": "dynamic_strategy_data_quality_gap_matrix.v1",
            "data_quality_gap_review": payload.get("data_quality_gap_review", {}),
            "prioritized_gap_matrix": [
                row
                for row in _as_list(payload.get("prioritized_gap_matrix"))
                if _as_mapping(row).get("gap_category") == "DATA_QUALITY"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    for path_key, report_type, schema_key, payload_key in (
        (
            "pit_coverage_gap_review_json",
            "dynamic_strategy_pit_coverage_gap_review",
            "dynamic_strategy_pit_coverage_gap_review.v1",
            "pit_coverage_gap_review",
        ),
        (
            "signal_quality_gap_review_json",
            "dynamic_strategy_signal_quality_gap_review",
            "dynamic_strategy_signal_quality_gap_review.v1",
            "signal_quality_gap_review",
        ),
        (
            "regime_labeling_gap_review_json",
            "dynamic_strategy_regime_labeling_gap_review",
            "dynamic_strategy_regime_labeling_gap_review.v1",
            "regime_labeling_gap_review",
        ),
        (
            "threshold_meta_dataset_gap_review_json",
            "dynamic_strategy_threshold_meta_dataset_gap_review",
            "dynamic_strategy_threshold_meta_dataset_gap_review.v1",
            "threshold_meta_dataset_gap_review",
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
        Path(paths["data_quality_gap_matrix_markdown"]),
        _data_quality_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["pit_coverage_gap_review_markdown"]),
        _pit_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["signal_quality_gap_review_markdown"]),
        _signal_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy data / PIT / signal quality gap review",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            (
                f"- validate-data：`{payload.get('validate_data_status')}`；"
                f"errors=`{payload.get('validate_data_error_count')}`；"
                f"warnings=`{payload.get('validate_data_warning_count')}`"
            ),
            f"- recombination line paused：`{payload.get('recombination_line_paused')}`",
            (
                "- resume candidate search recommended："
                f"`{payload.get('resume_candidate_search_recommended')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Source findings from TRADING-2401",
            "",
            f"- owner decision：`{payload.get('owner_decision_from_2401')}`",
            "- 2401 已确认 plateau，2402 只做 gap review，不恢复候选搜索。",
            "",
            "## Data quality review",
            "",
            _json_block(payload.get("data_quality_gap_review", {})),
            "",
            "## PIT coverage review",
            "",
            _json_block(payload.get("pit_coverage_gap_review", {})),
            "",
            "## Signal quality review",
            "",
            _json_block(payload.get("signal_quality_gap_review", {})),
            "",
            "## Valid-until / stale signal review",
            "",
            _json_block(
                _as_mapping(payload.get("signal_quality_gap_review")).get(
                    "valid_until_strictness", {}
                )
            ),
            "",
            "## Regime labeling review",
            "",
            _json_block(payload.get("regime_labeling_gap_review", {})),
            "",
            "## Threshold meta-dataset review",
            "",
            _json_block(payload.get("threshold_meta_dataset_gap_review", {})),
            "",
            "## Prioritized gap matrix",
            "",
            _json_block(payload.get("prioritized_gap_matrix", [])),
            "",
            "## Recommended next direction",
            "",
            _json_block(payload.get("next_research_direction_decision", {})),
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
        ]
    )


def _data_quality_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy data quality gap matrix",
            "",
            f"- status：`{payload.get('status')}`",
            f"- validate-data：`{payload.get('validate_data_status')}`",
            "",
            "## Data quality review",
            "",
            _json_block(payload.get("data_quality_gap_review", {})),
            "",
            "## DATA_QUALITY gaps",
            "",
            _json_block(
                [
                    row
                    for row in _as_list(payload.get("prioritized_gap_matrix"))
                    if _as_mapping(row).get("gap_category") == "DATA_QUALITY"
                ]
            ),
            "",
        ]
    )


def _pit_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT coverage gap review",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            _json_block(payload.get("pit_coverage_gap_review", {})),
            "",
        ]
    )


def _signal_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy signal quality gap review",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            _json_block(payload.get("signal_quality_gap_review", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2403 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            (
                "- resume candidate search recommended："
                f"`{payload.get('resume_candidate_search_recommended')}`"
            ),
            (
                "- research-only observation approved："
                f"`{payload.get('research_only_observation_approved')}`"
            ),
            f"- paper-shadow enabled：`{payload.get('paper_shadow_enabled')}`",
            f"- production enabled：`{payload.get('production_enabled')}`",
            f"- broker action enabled：`{payload.get('broker_action_enabled')}`",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any, default: str = "") -> str:
    return str(value) if value not in (None, "") else default


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
