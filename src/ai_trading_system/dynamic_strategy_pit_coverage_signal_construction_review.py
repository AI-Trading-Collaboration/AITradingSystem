from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_data_pit_signal_quality_gap_review as m2402
import ai_trading_system.dynamic_strategy_execution_cadence_bias_audit as m2364
import ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest as m2386
import ai_trading_system.dynamic_strategy_recombination_line_plateau_decision as m2401
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

TASK_ID = "TRADING-2403"
TASK_REGISTER_ID = (
    "TRADING-2403_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW"
)
REPORT_TYPE = "dynamic_strategy_pit_coverage_signal_construction_review"
SCHEMA_VERSION = "dynamic_strategy_pit_coverage_signal_construction_review.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_"
    "BLOCKED_SOURCE_ARTIFACT"
)
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_"
    "BLOCKED_DATA_QUALITY"
)
NEXT_ROUTE = "TRADING-2404_Dynamic_Strategy_PIT_Coverage_Matrix_Implementation_Plan"
DEFAULT_DATA_QUALITY_AS_OF = date(2026, 7, 5)
VALIDATE_DATA_AUDIT_DIR = PROJECT_ROOT / "artifacts" / "data_refresh_audit" / "validation"

SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2364",
    "TRADING-2386",
    "TRADING-2399",
    "TRADING-2401",
    "TRADING-2402",
)
DEFAULT_RECOMMENDED_OPTIONS: tuple[str, ...] = (
    "OPTION_A_BUILD_PIT_COVERAGE_MATRIX_IMPLEMENTATION",
    "OPTION_B_REVIEW_AND_REFACTOR_SIGNAL_CONSTRUCTION",
    "OPTION_C_BUILD_REGIME_EXPECTATION_SCORING",
    "OPTION_D_BUILD_THRESHOLD_META_DATASET",
)
EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
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
    "candidate_search_resumed",
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

DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2402_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "gap_review_result.json"
)
DEFAULT_SOURCE_2402_PIT_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "pit_coverage_gap_review.json"
)
DEFAULT_SOURCE_2402_SIGNAL_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "signal_quality_gap_review.json"
)
DEFAULT_SOURCE_2402_REGIME_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "regime_labeling_gap_review.json"
)
DEFAULT_SOURCE_2402_THRESHOLD_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "threshold_meta_dataset_gap_review.json"
)
DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH = (
    m2401.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT
    / "plateau_decision_result.json"
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


def run_dynamic_strategy_pit_coverage_signal_construction_review(
    *,
    source_gap_review_2402_path: Path = DEFAULT_SOURCE_2402_GAP_REVIEW_PATH,
    source_pit_gap_review_2402_path: Path = DEFAULT_SOURCE_2402_PIT_GAP_REVIEW_PATH,
    source_signal_gap_review_2402_path: Path = (
        DEFAULT_SOURCE_2402_SIGNAL_GAP_REVIEW_PATH
    ),
    source_regime_gap_review_2402_path: Path = (
        DEFAULT_SOURCE_2402_REGIME_GAP_REVIEW_PATH
    ),
    source_threshold_gap_review_2402_path: Path = (
        DEFAULT_SOURCE_2402_THRESHOLD_GAP_REVIEW_PATH
    ),
    source_plateau_decision_2401_path: Path = DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH,
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
        DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    validate_data_as_of: date = DEFAULT_DATA_QUALITY_AS_OF,
) -> dict[str, Any]:
    sources = _load_sources(
        source_gap_review_2402_path=source_gap_review_2402_path,
        source_pit_gap_review_2402_path=source_pit_gap_review_2402_path,
        source_signal_gap_review_2402_path=source_signal_gap_review_2402_path,
        source_regime_gap_review_2402_path=source_regime_gap_review_2402_path,
        source_threshold_gap_review_2402_path=source_threshold_gap_review_2402_path,
        source_plateau_decision_2401_path=source_plateau_decision_2401_path,
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
    source_gap_review_2402_path: Path,
    source_pit_gap_review_2402_path: Path,
    source_signal_gap_review_2402_path: Path,
    source_regime_gap_review_2402_path: Path,
    source_threshold_gap_review_2402_path: Path,
    source_plateau_decision_2401_path: Path,
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
        "gap_review_2402": _load_json_document(source_gap_review_2402_path),
        "pit_gap_review_2402": _load_json_document(source_pit_gap_review_2402_path),
        "signal_gap_review_2402": _load_json_document(source_signal_gap_review_2402_path),
        "regime_gap_review_2402": _load_json_document(source_regime_gap_review_2402_path),
        "threshold_gap_review_2402": _load_json_document(
            source_threshold_gap_review_2402_path
        ),
        "plateau_decision_2401": _load_json_document(source_plateau_decision_2401_path),
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
        "gap_review_2402": str(source_gap_review_2402_path),
        "pit_gap_review_2402": str(source_pit_gap_review_2402_path),
        "signal_gap_review_2402": str(source_signal_gap_review_2402_path),
        "regime_gap_review_2402": str(source_regime_gap_review_2402_path),
        "threshold_gap_review_2402": str(source_threshold_gap_review_2402_path),
        "plateau_decision_2401": str(source_plateau_decision_2401_path),
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
        "gap_review_2402": m2402.READY_STATUS,
        "pit_gap_review_2402": m2402.READY_STATUS,
        "signal_gap_review_2402": m2402.READY_STATUS,
        "regime_gap_review_2402": m2402.READY_STATUS,
        "threshold_gap_review_2402": m2402.READY_STATUS,
        "plateau_decision_2401": m2401.READY_STATUS,
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
    gap_2402 = _as_mapping(sources.get("gap_review_2402"))
    plateau_2401 = _as_mapping(sources.get("plateau_decision_2401"))
    targeted_2399 = _as_mapping(sources.get("targeted_retest_2399"))
    decision_2399 = _as_mapping(
        _as_mapping(sources.get("decision_update_2399")).get("decision_update")
    )
    if gap_2402.get("recommended_next_research_task") != m2402.NEXT_ROUTE:
        errors.append("2402 next route mismatch")
    if gap_2402.get("resume_candidate_search_recommended") is not False:
        errors.append("2402 unexpectedly recommended candidate search resume")
    if gap_2402.get("pit_coverage_matrix_recommended") is not True:
        errors.append("2402 did not recommend PIT coverage matrix")
    if gap_2402.get("signal_construction_review_recommended") is not True:
        errors.append("2402 did not recommend signal construction review")
    if plateau_2401.get("owner_decision") != m2401.OWNER_DECISION:
        errors.append("2401 owner decision mismatch")
    if plateau_2401.get("recombination_line_plateau_detected") is not True:
        errors.append("2401 plateau was not detected")
    if targeted_2399.get("best_targeted_variant") != m2401.BEST_TARGETED_VARIANT:
        errors.append("2399 best targeted variant mismatch")
    if targeted_2399.get("observation_preview_candidates_count") != 0:
        errors.append("2399 observation preview count mismatch")
    if decision_2399.get("research_only_observation_preview_exists") is True:
        errors.append("2399 unexpectedly found observation preview candidate")
    _validate_source_safety(sources, errors)
    return errors


def _validate_source_safety(sources: Mapping[str, Any], errors: list[str]) -> None:
    for source_name in (
        "gap_review_2402",
        "plateau_decision_2401",
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
        "data_quality_status": _validate_data_status(validate_audit),
        "validate_data_error_count": _int_value(validate_audit.get("error_count")),
        "data_quality_error_count": _int_value(validate_audit.get("error_count")),
        "validate_data_warning_count": _int_value(validate_audit.get("warning_count")),
        "data_quality_warning_count": _int_value(validate_audit.get("warning_count")),
        "validate_data_info_count": _int_value(validate_audit.get("info_count")),
        "validate_data_report_path": validate_audit.get("report_path"),
        "data_quality_gate_executed": True,
        "data_quality_gate_command": f"aits validate-data --as-of {validate_data_as_of}",
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "fresh_market_data_read_by_2403": False,
        "owner_decision_from_2401": m2401.OWNER_DECISION,
        "recombination_line_paused": True,
        "candidate_search_resumed": False,
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
    data_quality = _data_quality_warning_review(sources)
    pit_matrix = _pit_coverage_matrix(sources, data_quality)
    signal_review = _signal_construction_review(sources)
    valid_until_review = _valid_until_stale_signal_review(sources)
    regime_review = _regime_labeling_review(sources)
    threshold_gap = _threshold_meta_dataset_gap(sources)
    remediation = _remediation_matrix(
        data_quality=data_quality,
        pit_matrix=pit_matrix,
        signal_review=signal_review,
        valid_until_review=valid_until_review,
        regime_review=regime_review,
        threshold_gap=threshold_gap,
    )
    next_decision = _next_research_direction_decision()
    return {
        "pit_coverage_matrix_ready": True,
        "signal_construction_review_ready": True,
        "valid_until_stale_signal_review_ready": True,
        "regime_labeling_review_ready": True,
        "threshold_meta_dataset_gap_ready": True,
        "prioritized_remediation_matrix_ready": True,
        "pit_coverage_matrix": pit_matrix,
        "data_quality_warning_review": data_quality,
        "signal_construction_review": signal_review,
        "valid_until_stale_signal_review": valid_until_review,
        "regime_labeling_review": regime_review,
        "threshold_meta_dataset_gap": threshold_gap,
        "prioritized_remediation_matrix": remediation,
        "next_research_direction_decision": next_decision,
        "recommended_next_research_task": NEXT_ROUTE,
        "pit_coverage_matrix_implementation_plan_recommended": True,
        "signal_construction_refactor_recommended": True,
        "regime_expectation_scoring_recommended": True,
        "threshold_meta_dataset_recommended": True,
        "candidate_retest_resume_recommended": False,
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "pit_coverage_matrix_ready": False,
        "signal_construction_review_ready": False,
        "valid_until_stale_signal_review_ready": False,
        "regime_labeling_review_ready": False,
        "threshold_meta_dataset_gap_ready": False,
        "prioritized_remediation_matrix_ready": False,
        "pit_coverage_matrix": [],
        "data_quality_warning_review": {},
        "signal_construction_review": {},
        "valid_until_stale_signal_review": {},
        "regime_labeling_review": {},
        "threshold_meta_dataset_gap": {},
        "prioritized_remediation_matrix": [],
        "next_research_direction_decision": {},
        "recommended_next_research_task": None,
        "pit_coverage_matrix_implementation_plan_recommended": False,
        "signal_construction_refactor_recommended": False,
        "regime_expectation_scoring_recommended": False,
        "threshold_meta_dataset_recommended": False,
        "candidate_retest_resume_recommended": False,
    }


def _data_quality_warning_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    audit = _as_mapping(sources.get("validate_data_audit"))
    issues = _as_list(sources.get("validate_data_report_issues"))
    warning_rows = [row for row in issues if _text(row.get("severity")) == "警告"]
    classified = [_classify_data_quality_issue(row) for row in warning_rows]
    file_summaries = _as_mapping(audit.get("file_summaries"))
    price = _as_mapping(file_summaries.get("price_data"))
    secondary = _as_mapping(file_summaries.get("secondary_price_data"))
    rates = _as_mapping(file_summaries.get("macro_rate_data"))
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_2403_data_quality_warning_review.v1",
        "latest_validate_data_status": _validate_data_status(audit),
        "error_count": _int_value(audit.get("error_count")),
        "warning_count": _int_value(audit.get("warning_count")),
        "info_count": _int_value(audit.get("info_count")),
        "cached_data_coverage": {
            "price_min_date": price.get("min_date"),
            "price_max_date": price.get("max_date"),
            "price_rows": price.get("rows"),
            "secondary_price_min_date": secondary.get("min_date"),
            "secondary_price_max_date": secondary.get("max_date"),
            "secondary_price_rows": secondary.get("rows"),
            "macro_rate_min_date": rates.get("min_date"),
            "macro_rate_max_date": rates.get("max_date"),
            "macro_rate_rows": rates.get("rows"),
        },
        "warning_detail_summary": classified,
        "warnings_classified_as_dynamic_strategy_relevant_caveat": any(
            row.get("dynamic_strategy_relevance") == "MATERIAL" for row in classified
        ),
        "interpretation": (
            "PASS_WITH_WARNINGS does not block 2403 matrix construction, but the "
            "price manifest and TQQQ adjustment warning remain material caveats "
            "for any dynamic strategy candidate interpretation."
        ),
    }


def _classify_data_quality_issue(row: Mapping[str, Any]) -> dict[str, Any]:
    code = _text(row.get("code"))
    if code == "prices_download_manifest_checksum_missing":
        return {
            **dict(row),
            "gap_category": "DATA_QUALITY_WARNING",
            "dynamic_strategy_relevance": "MATERIAL",
            "pit_impact": "CACHE_PROVENANCE_CAVEAT",
            "recommended_action": (
                "reconcile price cache checksum with download_manifest before "
                "using the cache as promotion-quality evidence"
            ),
        }
    if code == "prices_adjustment_ratio_jump":
        return {
            **dict(row),
            "gap_category": "DATA_QUALITY_WARNING",
            "dynamic_strategy_relevance": "MATERIAL",
            "pit_impact": "ADJUSTED_PRICE_INTERPRETATION_CAVEAT",
            "recommended_action": (
                "review TQQQ corporate-action / adjusted-close basis and document "
                "whether the jump is vendor-basis or cache error"
            ),
        }
    return {
        **dict(row),
        "gap_category": "DATA_QUALITY_WARNING",
        "dynamic_strategy_relevance": "LOW",
        "pit_impact": "DISCLOSURE_ONLY",
        "recommended_action": "keep disclosed in data quality report",
    }


def _pit_coverage_matrix(
    sources: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> list[dict[str, Any]]:
    warnings = _as_list(data_quality.get("warning_detail_summary"))
    has_adjustment_warning = any(
        row.get("code") == "prices_adjustment_ratio_jump" for row in warnings
    )
    rows = [
        _pit_row(
            "market_prices",
            "market_data",
            "data/raw/prices_daily.csv; validate-data audit",
            "all dynamic candidates",
            ["TRADING-2386", "TRADING-2399", "TRADING-2402", "TRADING-2403"],
            "validated by as-of quality gate; historical rows only",
            "audit start/end timestamps recorded by validate-data",
            "TRUE_PIT",
            "HIGH",
            "LOW",
            "LOW",
            "MINOR",
            "LOW",
            "MATERIAL",
            "INFO",
            "keep as core market data input; retain validate-data link in reports",
        ),
        _pit_row(
            "adjusted_prices",
            "market_data",
            "data/raw/prices_daily.csv adjusted close fields",
            "QQQ/TQQQ/SGOV return and drawdown features",
            ["TRADING-2386", "TRADING-2399", "TRADING-2403"],
            "historical adjusted rows are as-of-gated; corporate action basis review pending",
            "generated by provider/cache path before research run",
            "APPROXIMATE_PIT",
            "MEDIUM",
            "LOW",
            "LOW",
            "MATERIAL" if has_adjustment_warning else "MINOR",
            "LOW",
            "MATERIAL",
            "MATERIAL" if has_adjustment_warning else "MINOR",
            "resolve TQQQ adjustment-ratio warning before promotion-quality ranking",
        ),
        _pit_row(
            "volume",
            "market_data",
            "price cache volume columns if present",
            "not primary candidate score; possible turnover diagnostics",
            ["TRADING-2386", "TRADING-2399"],
            "not separately asserted in 2402 gap review",
            "not normalized into PIT lineage matrix",
            "UNKNOWN",
            "LOW",
            "UNKNOWN",
            "LOW",
            "UNKNOWN",
            "UNKNOWN",
            "MINOR",
            "MINOR",
            "include volume field lineage only if future signal uses it",
        ),
        _pit_row(
            "returns",
            "technical_features",
            "derived from adjusted prices",
            "growth tilt, static baseline comparison, gate metrics",
            ["TRADING-2386", "TRADING-2399", "TRADING-2403"],
            "derived from historical price rows; window boundary needs explicit matrix",
            "feature generated during research run",
            "APPROXIMATE_PIT",
            "MEDIUM",
            "LOW",
            "LOW",
            "MATERIAL" if has_adjustment_warning else "MINOR",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "record feature window start/end and adjusted-price basis per candidate",
        ),
        _pit_row(
            "volatility_inputs",
            "technical_features",
            "rolling price-derived volatility features",
            "vol target / risk and regime diagnostics",
            ["TRADING-2364", "TRADING-2386", "TRADING-2399"],
            "rolling windows appear historical but explicit as-of lineage is missing",
            "not normalized into feature-level PIT manifest",
            "APPROXIMATE_PIT",
            "MEDIUM",
            "MATERIAL",
            "LOW",
            "MINOR",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "add window end-date and no-forward-fill assertion to PIT matrix",
        ),
        _pit_row(
            "trend_features",
            "technical_features",
            "historical price trend / momentum windows",
            "growth tilt and regime labels",
            ["TRADING-2386", "TRADING-2399"],
            "historical windows likely PIT, but feature-level lineage is not explicit",
            "not normalized into reusable manifest",
            "APPROXIMATE_PIT",
            "MEDIUM",
            "MATERIAL",
            "LOW",
            "MINOR",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "make trend feature windows explicit before observation review",
        ),
        _pit_row(
            "drawdown_features",
            "technical_features",
            "historical drawdown windows",
            "drawdown materiality gate and risk-off evidence",
            ["TRADING-2386", "TRADING-2399"],
            "research artifacts use drawdown evidence but PIT field lineage is missing",
            "generated after candidate retest",
            "APPROXIMATE_PIT",
            "MEDIUM",
            "MATERIAL",
            "LOW",
            "MINOR",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "separate live-available drawdown inputs from ex-post evaluation metrics",
        ),
        _pit_row(
            "growth_tilt_engine",
            "strategy_signals",
            "TRADING-2386 signal family screening; TRADING-2399 gate matrix",
            "growth_tilt family and guarded transfer variants",
            ["TRADING-2386", "TRADING-2399", "TRADING-2402"],
            "signal source features and horizon are not fully explicit",
            "not emitted as a standalone signal artifact",
            "UNKNOWN",
            "LOW",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "BLOCKING",
            "define source features, horizon, confidence, decay and valid-until lineage",
        ),
        _pit_row(
            "lower_turnover_guardrail",
            "strategy_signals",
            "candidate construction and targeted variants",
            "lower-turnover / guarded transfer candidates",
            ["TRADING-2396", "TRADING-2399", "TRADING-2403"],
            "guardrail uses research policy constants, not calibrated signal state",
            "candidate artifact generated during retest",
            "APPROXIMATE_PIT",
            "MEDIUM",
            "LOW",
            "LOW",
            "MINOR",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "promote turnover guardrail to explicit execution constraint policy",
        ),
        _pit_row(
            "valid_until_window",
            "execution_semantics",
            "valid_until_window / validity_10d_v1 family",
            "valid-until strict targeted variant",
            ["TRADING-2364", "TRADING-2386", "TRADING-2399", "TRADING-2403"],
            "currently a research execution assumption, not natural signal expiry evidence",
            "no standalone valid_from / valid_until lineage artifact",
            "APPROXIMATE_PIT",
            "LOW",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "BLOCKING",
            "build valid-from / valid-until PIT lineage and signal-age evidence",
        ),
        _pit_row(
            "signal_to_execution_lag",
            "execution_semantics",
            "TRADING-2399 execution metrics",
            "valid-until and stale-signal gates",
            ["TRADING-2399", "TRADING-2402", "TRADING-2403"],
            "lag appears in gate evidence but policy source is not normalized",
            "recorded after retest",
            "UNKNOWN",
            "LOW",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "define allowable lag and near-expiry handling before further retests",
        ),
        _pit_row(
            "stale_signal_detection",
            "execution_semantics",
            "TRADING-2399 valid_until_stale_signal_evidence",
            "no_stale_signal_carry_forward variants",
            ["TRADING-2399", "TRADING-2402", "TRADING-2403"],
            "stale count exists; detection rule is not yet reusable",
            "recorded in candidate gate matrix",
            "APPROXIMATE_PIT",
            "MEDIUM",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "extract stale signal detection into reusable signal audit",
        ),
        _pit_row(
            "regime_labels",
            "regime_labels",
            "risk_on/risk_off/high_volatility/low_volatility/trend_confirmed/recovery",
            "regime slice pass-rate and regime expectation evidence",
            ["TRADING-2386", "TRADING-2399", "TRADING-2403"],
            "labels are price-derived but rule timing is not fully normalized",
            "generated in retest artifacts",
            "APPROXIMATE_PIT",
            "LOW",
            "MATERIAL",
            "LOW",
            "MINOR",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "replace raw pass-rate with strategy-specific regime expectation scoring",
        ),
        _pit_row(
            "gate_inputs",
            "gate_inputs",
            "time/regime/drawdown/return/cost/turnover gate evidence",
            "owner review and continue-optimization decisions",
            ["TRADING-2386", "TRADING-2399", "TRADING-2402", "TRADING-2403"],
            "post-run evaluation metrics; not live signal inputs",
            "distributed across task-specific schemas",
            "NOT_APPLICABLE",
            "MEDIUM",
            "LOW_FOR_LIVE_SIGNAL_HIGH_FOR_RESEARCH_INTERPRETATION",
            "LOW",
            "LOW",
            "LOW",
            "MATERIAL",
            "MATERIAL",
            "normalize candidate x gate x decision history into meta-dataset",
        ),
    ]
    return rows


def _pit_row(
    input_id: str,
    input_type: str,
    source: str,
    used_by: str,
    tasks: list[str],
    as_of_handling: str,
    generated_at_handling: str,
    point_in_time_status: str,
    pit_confidence: str,
    lookahead_risk: str,
    revision_risk: str,
    stale_data_risk: str,
    missing_data_risk: str,
    relevance: str,
    severity: str,
    action: str,
) -> dict[str, Any]:
    return {
        "input_id": input_id,
        "input_type": input_type,
        "source_artifact_or_config": source,
        "used_by_candidate_or_signal": used_by,
        "used_by_tasks": tasks,
        "as_of_handling": as_of_handling,
        "generated_at_handling": generated_at_handling,
        "point_in_time_status": point_in_time_status,
        "pit_confidence": pit_confidence,
        "lookahead_risk": lookahead_risk,
        "revision_risk": revision_risk,
        "stale_data_risk": stale_data_risk,
        "missing_data_risk": missing_data_risk,
        "dynamic_strategy_relevance": relevance,
        "severity": severity,
        "recommended_action": action,
    }


def _signal_construction_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    gap_2402 = _as_mapping(sources.get("gap_review_2402"))
    signal_2402 = _as_mapping(
        _as_mapping(sources.get("signal_gap_review_2402")).get(
            "signal_quality_gap_review"
        )
    ) or _as_mapping(gap_2402.get("signal_quality_gap_review"))
    signal_family = _as_list(
        _as_mapping(sources.get("signal_family_screening_2386")).get(
            "signal_family_screening"
        )
    )
    best_signal_family = signal_family[0] if signal_family else {}
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_signal_construction_review.v1",
        "growth_tilt_engine": {
            "source_features": [
                "price-derived trend / momentum evidence",
                "growth_tilt_engine component from prior candidate family",
                "guarded_turnover_transfer evidence from recombination line",
            ],
            "pit_status": "UNKNOWN_UNTIL_FEATURE_LEVEL_LINEAGE_MATRIX_EXISTS",
            "signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
            "valid_until_rule": "validity_10d_v1 / valid_until_window family",
            "false_positive_risk": "MATERIAL_HIGH_VOLATILITY_RISK_ON_OVEREXPOSURE",
            "false_negative_risk": "MATERIAL_RECOVERY_REENTRY_LAG_OR_MISSED_UPSIDE",
            "return_contribution_evidence": best_signal_family,
            "drawdown_risk_evidence": (
                "TRADING-2399 gate evidence still requires drawdown materiality "
                "and regime expectation review"
            ),
            "recommended_fix": (
                "build standalone growth_tilt signal construction review with "
                "source features, horizon, confidence, decay and PIT lineage"
            ),
        },
        "turnover_budgeting": {
            "turnover_budget_source": "research policy / candidate construction",
            "cooldown_rule_source": "min-holding and cooldown variants from prior retests",
            "max_step_delta_source": "targeted variant pilot constants",
            "pit_status": "APPROXIMATE_PIT_POLICY_CONSTANTS_NOT_SIGNAL_CALIBRATED",
            "effect_on_cost_adjusted_return": (
                "guardrails can preserve cost-adjusted behavior but do not solve "
                "return-retention gap alone"
            ),
            "effect_on_return_gap": (
                "cooldown and lower-turnover constraints may cap growth tilt upside"
            ),
            "recommended_fix": (
                "separate turnover guardrail from signal alpha and manage it as an "
                "execution constraint"
            ),
        },
        "signal_reliability_conclusion": (
            "Current signal construction is reviewable but not reliable enough for "
            "observation approval because source feature lineage, signal confidence, "
            "natural expiry and regime-specific expected behavior are not normalized."
        ),
        "source_2402_signal_gap": signal_2402,
    }


def _valid_until_stale_signal_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    gate_matrix = _as_list(
        _as_mapping(sources.get("gate_evidence_matrix_2399")).get("gate_evidence_matrix")
    )
    best_row = _best_candidate_gate_row(gate_matrix, m2401.BEST_TARGETED_VARIANT)
    valid_until = _as_mapping(best_row.get("valid_until_stale_signal_evidence"))
    execution_metrics = _as_mapping(best_row.get("execution_metrics"))
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_valid_until_stale_signal_review.v1",
        "valid_until_source": (
            "research execution assumption from valid_until_window / validity_10d_v1"
        ),
        "valid_until_pit_status": "APPROXIMATE_PIT_NOT_NATURAL_SIGNAL_EXPIRY",
        "stale_signal_detection_rule": (
            "count stale executions from TRADING-2399 gate evidence; reusable rule "
            "not yet extracted"
        ),
        "stale_signal_execution_count": valid_until.get(
            "stale_signal_execution_count",
            execution_metrics.get("stale_signal_execution_count"),
        ),
        "signal_to_execution_lag_rule": (
            "lag appears in execution evidence but lacks formal allowed-lag policy"
        ),
        "signal_to_execution_lag_days": valid_until.get(
            "signal_to_execution_lag_days",
            execution_metrics.get("signal_to_execution_lag_days"),
        ),
        "near_expiry_decay_needed": True,
        "strict_expiry_needed": True,
        "no_stale_signal_carry_forward_review": "REQUIRED_BEFORE_OBSERVATION",
        "recommended_fix": (
            "build valid-from / valid-until lineage, signal-age buckets, near-expiry "
            "decay handling and stale-signal carry-forward assertions"
        ),
    }


def _regime_labeling_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    gate_matrix = _as_list(
        _as_mapping(sources.get("gate_evidence_matrix_2399")).get("gate_evidence_matrix")
    )
    best_row = _best_candidate_gate_row(gate_matrix, m2401.BEST_TARGETED_VARIANT)
    regime = _as_mapping(best_row.get("regime_expectation_evidence"))
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_regime_labeling_review.v1",
        "current_labels": [
            "risk_on",
            "risk_off",
            "high_volatility",
            "low_volatility",
            "trend_confirmed",
            "recovery",
        ],
        "review_questions": {
            "labels_from_explicit_rules": "PARTIAL_PRIOR_RULES_NOT_NORMALIZED",
            "label_point_in_time_safe": "UNKNOWN_UNTIL_RULE_TIMING_REVIEW",
            "uses_future_window_confirmation": "MATERIAL_REVIEW_REQUIRED",
            "should_growth_tilt_outperform_static_in_all_regimes": False,
            "different_expected_behavior_by_regime": True,
        },
        "regime_expectation_score_from_best_variant": regime.get(
            "regime_expectation_score"
        ),
        "regime_expectation_not_weak": regime.get("regime_expectation_not_weak"),
        "regime_expectation_mapping_proposal": {
            "risk_on": "outperform_static_or_retain_upside",
            "trend_confirmed": "capture_growth_tilt_return",
            "low_volatility": "allow_moderate_upside",
            "risk_off": "not_materially_worse_than_static",
            "high_volatility": "control_drawdown",
            "recovery": "avoid_excessive_reentry_lag",
        },
        "recommended_fix": (
            "replace raw regime_slice_pass_rate with strategy-specific "
            "regime_expectation_score and explicit PIT label rules"
        ),
    }


def _threshold_meta_dataset_gap(sources: Mapping[str, Any]) -> dict[str, Any]:
    decision_2399 = _as_mapping(
        _as_mapping(sources.get("decision_update_2399")).get("decision_update")
    )
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_threshold_meta_dataset_gap.v1",
        "required_fields": [
            "candidate_id",
            "source_task",
            "candidate_family",
            "execution_cadence",
            "cost_stress_level",
            "dynamic_vs_static_gap",
            "cost_adjusted_return",
            "max_drawdown",
            "drawdown_gap_vs_static",
            "turnover",
            "time_slice_pass_rate",
            "regime_slice_pass_rate",
            "regime_expectation_score",
            "decision_before_calibration",
            "decision_after_calibration",
            "owner_review_required",
            "observation_preview",
            "final_owner_decision",
        ],
        "current_status": {
            "candidate_results_distributed_across_task_schemas": True,
            "normalized_meta_dataset_exists": False,
            "current_2399_candidate_decisions": decision_2399.get("candidate_decisions"),
        },
        "needs_dedicated_2404_or_2405_task": True,
        "recommended_fix": (
            "build candidate x gate x decision meta-dataset before threshold "
            "calibration or candidate retest resume"
        ),
    }


def _remediation_matrix(
    *,
    data_quality: Mapping[str, Any],
    pit_matrix: list[dict[str, Any]],
    signal_review: Mapping[str, Any],
    valid_until_review: Mapping[str, Any],
    regime_review: Mapping[str, Any],
    threshold_gap: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    blocking_inputs = [
        row for row in pit_matrix if row.get("severity") == "BLOCKING"
    ]
    if blocking_inputs:
        rows.append(
            _remediation_row(
                "2403-PIT-01",
                "PIT_COVERAGE",
                "feature / signal / valid-until PIT lineage matrix is missing",
                "BLOCKING",
                ["dynamic strategy candidate family"],
                ["growth_tilt_engine", "valid_until_window"],
                SOURCE_TASKS,
                "implement reusable PIT coverage matrix and fail-closed report",
                "prevents lookahead ambiguity before any observation review",
                NEXT_ROUTE,
                True,
            )
        )
    rows.append(
        _remediation_row(
            "2403-SIGNAL-01",
            "SIGNAL_CONSTRUCTION",
            _text(signal_review.get("signal_reliability_conclusion")),
            "MATERIAL",
            [m2401.BASE_CANDIDATE, m2401.BEST_TARGETED_VARIANT],
            ["growth_tilt_engine", "lower_turnover_guardrail"],
            ["TRADING-2386", "TRADING-2399", "TRADING-2402"],
            "review and refactor signal construction after PIT matrix",
            "separates alpha signal quality from portfolio/execution guardrails",
            "TRADING-2405_Dynamic_Strategy_Signal_Construction_Review",
            True,
        )
    )
    rows.append(
        _remediation_row(
            "2403-VALIDUNTIL-01",
            "VALID_UNTIL_AND_STALE_SIGNAL",
            "valid_until_window is not yet backed by natural signal expiry evidence",
            "BLOCKING",
            [m2401.BEST_TARGETED_VARIANT],
            ["valid_until_window", "stale_signal_detection"],
            ["TRADING-2364", "TRADING-2399", "TRADING-2402"],
            _text(valid_until_review.get("recommended_fix")),
            "reduces stale-signal and missed-signal ambiguity",
            NEXT_ROUTE,
            True,
        )
    )
    rows.append(
        _remediation_row(
            "2403-REGIME-01",
            "REGIME_LABELING",
            "regime labels are too coarse for growth / risk-on expected behavior",
            "MATERIAL",
            ["growth_tilt strategy family"],
            ["regime_labels", "regime_expectation_score"],
            ["TRADING-2386", "TRADING-2399", "TRADING-2402"],
            _text(regime_review.get("recommended_fix")),
            (
                "improves interpretation of regime failures without requiring "
                "all-regime outperformance"
            ),
            "TRADING-2406_Dynamic_Strategy_Regime_Expectation_Scoring_Review",
            True,
        )
    )
    rows.append(
        _remediation_row(
            "2403-THRESHOLD-01",
            "THRESHOLD_META_DATASET",
            "threshold and gate decisions lack normalized candidate history",
            "MATERIAL",
            ["all dynamic strategy candidates"],
            ["gate_inputs"],
            SOURCE_TASKS,
            _text(threshold_gap.get("recommended_fix")),
            "makes owner-review vs continue-optimization boundary auditable",
            "TRADING-2407_Dynamic_Strategy_Threshold_Meta_Dataset_Build",
            True,
        )
    )
    for index, warning in enumerate(
        _as_list(data_quality.get("warning_detail_summary")), start=1
    ):
        if warning.get("dynamic_strategy_relevance") != "MATERIAL":
            continue
        rows.append(
            _remediation_row(
                f"2403-DATA-{index:02d}",
                "DATA_QUALITY_WARNING",
                _text(warning.get("description")),
                "MATERIAL",
                ["QQQ", "TQQQ", "SGOV"],
                ["market_prices", "adjusted_prices"],
                ["TRADING-2386", "TRADING-2399", "TRADING-2403"],
                _text(warning.get("recommended_action")),
                _text(warning.get("pit_impact")),
                NEXT_ROUTE,
                True,
            )
        )
    rows.append(
        _remediation_row(
            "2403-REPORTING-01",
            "REPORTING_NORMALIZATION",
            "evidence remains distributed across task-specific schemas",
            "MINOR",
            ["all dynamic strategy candidates"],
            ["all review artifacts"],
            SOURCE_TASKS,
            "normalize PIT, signal, regime and gate evidence in reusable reports",
            "reduces owner review overhead and prevents inconsistent caveat handling",
            NEXT_ROUTE,
            False,
        )
    )
    return rows


def _remediation_row(
    remediation_id: str,
    category: str,
    issue: str,
    severity: str,
    affected_candidates: list[str],
    affected_signals: list[str],
    affected_tasks: tuple[str, ...] | list[str],
    action: str,
    impact: str,
    next_task: str,
    owner_review_required: bool,
) -> dict[str, Any]:
    return {
        "remediation_id": remediation_id,
        "category": category,
        "issue": issue,
        "severity": severity,
        "affected_candidates": affected_candidates,
        "affected_signals": affected_signals,
        "affected_tasks": list(affected_tasks),
        "recommended_action": action,
        "expected_impact": impact,
        "recommended_next_task": next_task,
        "owner_review_required": owner_review_required,
    }


def _next_research_direction_decision() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_options": list(DEFAULT_RECOMMENDED_OPTIONS),
        "decision_options": {
            "OPTION_A_BUILD_PIT_COVERAGE_MATRIX_IMPLEMENTATION": {
                "meaning": "将 PIT coverage matrix 固化为可复用工具和报告",
                "recommended": True,
                "default_route": True,
            },
            "OPTION_B_REVIEW_AND_REFACTOR_SIGNAL_CONSTRUCTION": {
                "meaning": "重构 growth_tilt / valid_until / turnover / regime signal 构造",
                "recommended": True,
                "default_route": False,
            },
            "OPTION_C_BUILD_REGIME_EXPECTATION_SCORING": {
                "meaning": "替代粗糙 regime pass-rate",
                "recommended": True,
                "default_route": False,
            },
            "OPTION_D_BUILD_THRESHOLD_META_DATASET": {
                "meaning": "汇总历史 candidate x gate x decision，校准 threshold",
                "recommended": True,
                "default_route": False,
            },
            "OPTION_E_RESUME_CANDIDATE_RETEST": {
                "meaning": "恢复候选搜索",
                "recommended": False,
                "default_route": False,
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
        "json_path": str(output_root / "pit_signal_review_result.json"),
        "pit_coverage_matrix_json": str(output_root / "pit_coverage_matrix.json"),
        "signal_construction_review_json": str(
            output_root / "signal_construction_review.json"
        ),
        "regime_labeling_review_json": str(output_root / "regime_labeling_review.json"),
        "remediation_matrix_json": str(output_root / "remediation_matrix.json"),
        "threshold_meta_dataset_gap_json": str(
            output_root / "threshold_meta_dataset_gap.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_pit_coverage_signal_construction_review.md"
        ),
        "pit_coverage_matrix_markdown": str(
            docs_root / "dynamic_strategy_pit_coverage_matrix.md"
        ),
        "signal_construction_review_markdown": str(
            docs_root / "dynamic_strategy_signal_construction_review.md"
        ),
        "regime_labeling_review_markdown": str(
            docs_root / "dynamic_strategy_regime_labeling_review.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2404_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    for path_key, report_type, schema_key, payload_key in (
        (
            "pit_coverage_matrix_json",
            "dynamic_strategy_pit_coverage_matrix",
            "dynamic_strategy_pit_coverage_matrix.v1",
            "pit_coverage_matrix",
        ),
        (
            "signal_construction_review_json",
            "dynamic_strategy_signal_construction_review",
            "dynamic_strategy_signal_construction_review.v1",
            "signal_construction_review",
        ),
        (
            "regime_labeling_review_json",
            "dynamic_strategy_regime_labeling_review",
            "dynamic_strategy_regime_labeling_review.v1",
            "regime_labeling_review",
        ),
        (
            "remediation_matrix_json",
            "dynamic_strategy_pit_signal_remediation_matrix",
            "dynamic_strategy_pit_signal_remediation_matrix.v1",
            "prioritized_remediation_matrix",
        ),
        (
            "threshold_meta_dataset_gap_json",
            "dynamic_strategy_threshold_meta_dataset_gap",
            "dynamic_strategy_threshold_meta_dataset_gap.v1",
            "threshold_meta_dataset_gap",
        ),
    ):
        write_json_artifact(
            Path(paths[path_key]),
            {
                "task_id": TASK_ID,
                "status": payload.get("status"),
                "report_type": report_type,
                "schema_version": schema_key,
                payload_key: payload.get(payload_key, [] if "matrix" in payload_key else {}),
                "production_effect": "none",
                "broker_action": "none",
            },
        )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["pit_coverage_matrix_markdown"]),
        _pit_matrix_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["signal_construction_review_markdown"]),
        _signal_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["regime_labeling_review_markdown"]),
        _regime_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT coverage matrix and signal construction review",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            (
                f"- validate-data：`{payload.get('validate_data_status')}`；"
                f"errors=`{payload.get('validate_data_error_count')}`；"
                f"warnings=`{payload.get('validate_data_warning_count')}`"
            ),
            f"- candidate search resumed：`{payload.get('candidate_search_resumed')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Source findings from TRADING-2402",
            "",
            (
                "- 2402 已确认不恢复 candidate search，优先推进 PIT coverage matrix "
                "与 signal construction review。"
            ),
            "",
            "## PIT coverage matrix",
            "",
            _json_block(payload.get("pit_coverage_matrix", [])),
            "",
            "## Data quality warning interpretation",
            "",
            _json_block(payload.get("data_quality_warning_review", {})),
            "",
            "## Signal construction review",
            "",
            _json_block(payload.get("signal_construction_review", {})),
            "",
            "## Valid-until / stale signal review",
            "",
            _json_block(payload.get("valid_until_stale_signal_review", {})),
            "",
            "## Regime labeling review",
            "",
            _json_block(payload.get("regime_labeling_review", {})),
            "",
            "## Threshold meta-dataset gap",
            "",
            _json_block(payload.get("threshold_meta_dataset_gap", {})),
            "",
            "## Prioritized remediation matrix",
            "",
            _json_block(payload.get("prioritized_remediation_matrix", [])),
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


def _pit_matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT coverage matrix",
            "",
            f"- status：`{payload.get('status')}`",
            f"- matrix ready：`{payload.get('pit_coverage_matrix_ready')}`",
            "",
            _json_block(payload.get("pit_coverage_matrix", [])),
            "",
        ]
    )


def _signal_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy signal construction review",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- signal construction review ready："
                f"`{payload.get('signal_construction_review_ready')}`"
            ),
            (
                "- valid-until stale signal review ready："
                f"`{payload.get('valid_until_stale_signal_review_ready')}`"
            ),
            "",
            "## Signal construction",
            "",
            _json_block(payload.get("signal_construction_review", {})),
            "",
            "## Valid-until / stale signal",
            "",
            _json_block(payload.get("valid_until_stale_signal_review", {})),
            "",
        ]
    )


def _regime_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy regime labeling review",
            "",
            f"- status：`{payload.get('status')}`",
            f"- regime labeling review ready：`{payload.get('regime_labeling_review_ready')}`",
            "",
            _json_block(payload.get("regime_labeling_review", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2404 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            f"- candidate search resumed：`{payload.get('candidate_search_resumed')}`",
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
