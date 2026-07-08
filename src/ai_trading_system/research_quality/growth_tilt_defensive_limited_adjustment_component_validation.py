from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_defensive_limited_adjustment_component_validation.v1"
COMPONENT_VALUE_ASSESSMENT_SCHEMA_VERSION = (
    "growth_tilt_defensive_limited_adjustment_component_value_assessment.v1"
)
PRIMARY_VALUE_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_defensive_limited_adjustment_primary_value_matrix.v1"
)
VALIDATION_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_defensive_limited_adjustment_validation_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_DEFENSIVE_LIMITED_ADJUSTMENT_COMPONENT_VALIDATION_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_DEFENSIVE_LIMITED_ADJUSTMENT_COMPONENT_VALIDATION_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2433_STATUS = "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY"
EXPECTED_2433_NEXT_ROUTE = (
    "TRADING-2434_Defensive_Limited_Adjustment_Component_Validation"
)
NEXT_ROUTE = "TRADING-2435_Growth_Tilt_Valid_Until_Outcome_Hit_Rate_Study"
BLOCKED_ROUTE = (
    "TRADING-2434_Defensive_Limited_Adjustment_Component_Evidence_Gap_Remediation"
)
REPORT_TYPE = "growth_tilt_defensive_limited_adjustment_component_validation"
CANDIDATE_ID = "defensive_limited_adjustment_false_risk_off_reducer"
CANDIDATE_STATUS_VALUES: tuple[str, ...] = ("rejected", "component_value", "needs_pit")
PRIMARY_VALUE_IDS: tuple[str, ...] = (
    "drawdown_control",
    "false_risk_off_reduction",
    "missed_upside_reduction",
    "turnover_control",
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_false_risk_off_missed_upside_batch_screen",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-defensive-limited-adjustment-component-validation",
    "outputs/research_strategies/growth_tilt_defensive_limited_adjustment_component_validation/"
    "component_validation_result.json",
    "outputs/research_strategies/growth_tilt_defensive_limited_adjustment_component_validation/"
    "component_value_assessment.json",
    "outputs/research_strategies/growth_tilt_defensive_limited_adjustment_component_validation/"
    "primary_value_matrix.json",
    "outputs/research_strategies/growth_tilt_defensive_limited_adjustment_component_validation/"
    "validation_boundary.json",
    "docs/research/growth_tilt_defensive_limited_adjustment_component_validation.md",
    "docs/research/growth_tilt_defensive_limited_adjustment_component_value_assessment.md",
    "docs/research/growth_tilt_defensive_limited_adjustment_primary_value_matrix.md",
    "docs/research/growth_tilt_defensive_limited_adjustment_validation_boundary.md",
    "docs/research/dynamic_strategy_2435_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-defensive-limited-adjustment-component-validation",
    READY_STATUS,
    NEXT_ROUTE,
)


def build_growth_tilt_defensive_limited_adjustment_component_validation(
    source_2433_batch_screen: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    source_candidate = _source_candidate(source_2433_batch_screen)
    component_value_found = (
        _source_2433_ready(source_2433_batch_screen)
        and source_candidate.get("batch_decision") == "component_value"
    )
    candidate_status = "component_value" if component_value_found else "needs_pit"
    requirements = _requirements(
        source_2433_batch_screen,
        source_candidate,
        candidate_status,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_text=research_text,
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in requirements
        if requirement["status"] != "PASS"
    ]
    status = READY_STATUS if not gaps else BLOCKED_STATUS
    next_route = NEXT_ROUTE if status == READY_STATUS else BLOCKED_ROUTE
    assessment = _component_value_assessment(
        status,
        gaps,
        component_value_found=component_value_found and status == READY_STATUS,
        candidate_status=candidate_status,
        source_candidate=source_candidate,
    )
    primary_value_matrix = _primary_value_matrix(status, gaps)
    boundary = _validation_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2434",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2433", "TRADING-2431"],
        "source_2433_ready": _source_2433_ready(source_2433_batch_screen),
        "source_candidate_found": bool(source_candidate),
        "component_validation_ready": status == READY_STATUS,
        "component_value_assessment_ready": assessment[
            "component_value_assessment_ready"
        ],
        "primary_value_matrix_ready": primary_value_matrix["primary_value_matrix_ready"],
        "validation_boundary_ready": boundary["validation_boundary_ready"],
        "component_value_found": assessment["component_value_found"],
        "candidate_status": assessment["candidate_status"],
        "candidate_status_values": list(CANDIDATE_STATUS_VALUES),
        "primary_value": list(PRIMARY_VALUE_IDS),
        "promotion_candidate_found": False,
        "promotion_candidate_count": 0,
        "computed_new_metrics": False,
        "market_data_component_validation_run": False,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "component_value_assessment": assessment,
        "primary_value_matrix": primary_value_matrix,
        "validation_boundary": boundary,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "observe_only": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "Defensive limited adjustment has component value evidence; continue "
            "to valid-until outcome hit-rate study."
            if status == READY_STATUS
            else "Required component evidence or documentation coverage is missing."
        ),
    }


def _requirements(
    source_2433: Mapping[str, Any],
    source_candidate: Mapping[str, Any],
    candidate_status: str,
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2433_batch_screen_ready",
            _source_2433_ready(source_2433),
            "prior_screen_gap",
            {
                "status": source_2433.get("status"),
                "next_route": source_2433.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "defensive_limited_adjustment_candidate_found",
            bool(source_candidate),
            "candidate_evidence_gap",
            {"candidate_id": CANDIDATE_ID},
        ),
        _requirement(
            "candidate_status_valid",
            candidate_status in CANDIDATE_STATUS_VALUES,
            "candidate_status_gap",
            {"candidate_status": candidate_status},
        ),
        _requirement(
            "component_value_prior_evidence",
            source_candidate.get("batch_decision") == "component_value",
            "component_value_gap",
            {"batch_decision": source_candidate.get("batch_decision")},
        ),
        _requirement(
            "prior_research_doc_coverage",
            "defensive_limited_adjustment" in research_text,
            "research_doc_gap",
            {"required_reference": "defensive_limited_adjustment"},
        ),
        _requirement(
            "report_registry_coverage",
            _report_registry_has(report_registry, REQUIRED_REPORT_IDS),
            "registry_catalog_doc_gap",
            {"required_report_ids": list(REQUIRED_REPORT_IDS)},
        ),
        _requirement(
            "artifact_catalog_coverage",
            _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES),
            "registry_catalog_doc_gap",
            {"required_references": list(REQUIRED_CATALOG_REFERENCES)},
        ),
        _requirement(
            "system_flow_coverage",
            _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES),
            "registry_catalog_doc_gap",
            {"required_references": list(REQUIRED_SYSTEM_FLOW_REFERENCES)},
        ),
    ]


def _source_2433_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2433_STATUS
        and payload.get("batch_screen_ready") is True
        and payload.get("promotion_candidate_found") is False
        and payload.get("recommended_next_research_task") == EXPECTED_2433_NEXT_ROUTE
    )


def _source_candidate(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    matrix = _mapping(payload.get("candidate_screen_matrix"))
    candidates = matrix.get("candidates")
    if not isinstance(candidates, Sequence) or isinstance(candidates, str):
        return {}
    for row in candidates:
        if isinstance(row, Mapping) and row.get("candidate_id") == CANDIDATE_ID:
            return row
    return {}


def _component_value_assessment(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
    *,
    component_value_found: bool,
    candidate_status: str,
    source_candidate: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": COMPONENT_VALUE_ASSESSMENT_SCHEMA_VERSION,
        "status": status,
        "component_value_assessment_ready": not gaps,
        "candidate_id": CANDIDATE_ID,
        "component_value_found": component_value_found,
        "candidate_status": candidate_status,
        "source_batch_decision": source_candidate.get("batch_decision"),
        "promotion_candidate_found": False,
        "computed_new_metrics": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _primary_value_matrix(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": PRIMARY_VALUE_MATRIX_SCHEMA_VERSION,
        "status": status,
        "primary_value_matrix_ready": not gaps,
        "primary_values": [
            {
                "primary_value_id": value,
                "supported_as_component_value": not gaps,
                "computed_in_2434": False,
            }
            for value in PRIMARY_VALUE_IDS
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _validation_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": VALIDATION_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "validation_boundary_ready": not gaps,
        "component_validation_only": True,
        "promotion_candidate_found": False,
        "fresh_market_data_read": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "generated_signal": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "evidence_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _requirement(
    requirement_id: str,
    passed: bool,
    classification: str,
    evidence: Any,
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "status": "PASS" if passed else "FAIL",
        "classification": classification,
        "evidence": evidence,
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requirement_id": requirement["requirement_id"],
        "classification": requirement["classification"],
        "gap": f"{requirement['requirement_id']} did not pass.",
        "evidence": requirement.get("evidence"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    reports = report_registry.get("reports")
    if not isinstance(reports, Sequence):
        return False
    present = {
        str(report.get("report_id"))
        for report in reports
        if isinstance(report, Mapping) and report.get("report_id")
    }
    return set(report_ids).issubset(present)


def _contains_all(text: str, references: Sequence[str]) -> bool:
    return all(reference in text for reference in references)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
