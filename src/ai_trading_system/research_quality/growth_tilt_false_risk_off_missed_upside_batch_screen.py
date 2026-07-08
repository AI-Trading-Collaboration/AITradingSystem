from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_false_risk_off_missed_upside_batch_screen.v1"
CANDIDATE_SCREEN_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_false_risk_off_missed_upside_candidate_screen_matrix.v1"
)
BATCH_DECISION_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_false_risk_off_missed_upside_batch_decision_summary.v1"
)
RESEARCH_QUESTION_COVERAGE_SCHEMA_VERSION = (
    "growth_tilt_false_risk_off_missed_upside_research_question_coverage.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_false_risk_off_missed_upside_no_effect_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_BLOCKED_BY_SCREEN_CONTRACT_GAPS"
)
EXPECTED_2432_STATUS = "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
EXPECTED_2432_NEXT_ROUTE = (
    "TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen"
)
EXPECTED_2432_CANDIDATE_SET_ID = "growth_tilt_batch_2432"
NEXT_ROUTE = "TRADING-2434_Defensive_Limited_Adjustment_Component_Validation"
BLOCKED_ROUTE = (
    "TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Screen_Gap_Remediation"
)
REPORT_TYPE = "growth_tilt_false_risk_off_missed_upside_batch_screen"
CANDIDATE_SET_ID = "false_risk_off_missed_upside_2433"

DECISION_VALUES: tuple[str, ...] = (
    "rejected",
    "component_value",
    "pit_candidate",
    "promotion_candidate",
)
RESEARCH_QUESTIONS: tuple[str, ...] = (
    "over_defensive_entry",
    "slow_growth_recovery_reentry",
    "false_defensive_day_reduction",
    "missed_upside_without_drawdown_damage",
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_candidate_gauntlet_harness",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "research/configs/growth_tilt/false_risk_off_missed_upside_2433.yaml",
    "aits research strategies growth-tilt-false-risk-off-missed-upside-batch-screen",
    "outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/"
    "batch_screen_result.json",
    "outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/"
    "candidate_screen_matrix.json",
    "outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/"
    "batch_decision_summary.json",
    "outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/"
    "research_question_coverage.json",
    "outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_false_risk_off_missed_upside_batch_screen.md",
    "docs/research/growth_tilt_false_risk_off_missed_upside_candidate_screen_matrix.md",
    "docs/research/growth_tilt_false_risk_off_missed_upside_batch_decision_summary.md",
    "docs/research/growth_tilt_false_risk_off_missed_upside_research_question_coverage.md",
    "docs/research/growth_tilt_false_risk_off_missed_upside_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2434_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-false-risk-off-missed-upside-batch-screen",
    CANDIDATE_SET_ID,
    READY_STATUS,
    NEXT_ROUTE,
)


def build_growth_tilt_false_risk_off_missed_upside_batch_screen(
    source_2432_candidate_gauntlet_harness: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    rows = [_candidate_row(row) for row in _sequence(candidate_set.get("candidates"))]
    question_coverage = _research_question_coverage(rows)
    requirements = _requirements(
        source_2432_candidate_gauntlet_harness,
        candidate_set,
        rows,
        question_coverage,
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
    decision_counts = Counter(row["batch_decision"] for row in rows)
    matrix = _candidate_screen_matrix(rows, status=status, gaps=gaps)
    summary = _batch_decision_summary(rows, status=status, gaps=gaps, next_route=next_route)
    coverage = _coverage_section(question_coverage, status=status, gaps=gaps)
    no_effect_boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2433",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2432", "TRADING-2431"],
        "source_2432_ready": _source_2432_ready(source_2432_candidate_gauntlet_harness),
        "candidate_set_ready": _candidate_set_ready(candidate_set, rows),
        "candidate_set_id": str(candidate_set.get("candidate_set_id", "")),
        "batch_screen_ready": not gaps,
        "candidate_screen_matrix_ready": matrix["candidate_screen_matrix_ready"],
        "batch_decision_summary_ready": summary["batch_decision_summary_ready"],
        "research_question_coverage_ready": coverage[
            "research_question_coverage_ready"
        ],
        "no_effect_boundary_ready": no_effect_boundary["no_effect_boundary_ready"],
        "candidate_count": len(rows),
        "candidates_screened": len(rows),
        "rejected_count": decision_counts["rejected"],
        "component_value_count": decision_counts["component_value"],
        "pit_candidate_count": decision_counts["pit_candidate"],
        "promotion_candidate_count": decision_counts["promotion_candidate"],
        "promotion_candidate_found": decision_counts["promotion_candidate"] > 0,
        "research_question_count": len(RESEARCH_QUESTIONS),
        "research_question_covered_count": sum(
            1 for row in question_coverage if row["covered"] is True
        ),
        "new_investment_threshold_values_set": False,
        "threshold_policy_required_for_pit_or_promotion": True,
        "criteria_threshold_values_all_null": _criteria_thresholds_all_null(candidate_set),
        "computed_new_metrics": False,
        "market_data_candidate_screen_run": False,
        "candidate_batch_screen_run": status == READY_STATUS,
        "requirements": requirements,
        "gaps": gaps,
        "screen_contract_gap_count": len(gaps),
        "screen_contract_gap_ids": [gap["requirement_id"] for gap in gaps],
        "candidate_screen_matrix": matrix,
        "batch_decision_summary": summary,
        "research_question_coverage": coverage,
        "no_effect_boundary": no_effect_boundary,
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
            "False risk-off and missed-upside candidate triage is ready; continue "
            "to defensive limited adjustment component validation."
            if status == READY_STATUS
            else "Required screen contract, harness evidence, or documentation coverage is missing."
        ),
    }


def _requirements(
    source_2432: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    question_coverage: Sequence[Mapping[str, Any]],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2432_candidate_gauntlet_harness_ready",
            _source_2432_ready(source_2432),
            "prior_harness_gap",
            {
                "status": source_2432.get("status"),
                "candidate_set_id": source_2432.get("candidate_set_id"),
                "next_route": source_2432.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "candidate_set_id_ready",
            candidate_set.get("candidate_set_id") == CANDIDATE_SET_ID,
            "candidate_set_gap",
            {"candidate_set_id": candidate_set.get("candidate_set_id")},
        ),
        _requirement(
            "candidate_set_safety_boundary",
            _candidate_set_safety_boundary_ready(candidate_set),
            "safety_boundary_gap",
            dict(_mapping(candidate_set.get("safety_boundary"))),
        ),
        _requirement(
            "candidate_rows_ready",
            bool(rows) and all(row["batch_decision"] in DECISION_VALUES for row in rows),
            "candidate_set_gap",
            {"candidate_count": len(rows), "decision_values": list(DECISION_VALUES)},
        ),
        _requirement(
            "promotion_candidate_not_allowed_by_default",
            not any(row["batch_decision"] == "promotion_candidate" for row in rows),
            "promotion_governance_gap",
            {"promotion_candidate_count": _decision_count(rows, "promotion_candidate")},
        ),
        _requirement(
            "research_question_coverage",
            all(row.get("covered") is True for row in question_coverage),
            "research_question_gap",
            {"required_questions": list(RESEARCH_QUESTIONS)},
        ),
        _requirement(
            "criteria_threshold_governance",
            _criteria_thresholds_all_null(candidate_set),
            "heuristic_governance_gap",
            {
                "new_investment_threshold_values_set": False,
                "threshold_policy_required_for_pit_or_promotion": True,
            },
        ),
        _requirement(
            "prior_research_doc_coverage",
            _research_doc_has_harness_reference(research_text),
            "research_doc_gap",
            {
                "required_reference": (
                    "growth_tilt_candidate_gauntlet_harness or "
                    "Growth Tilt Candidate Gauntlet Harness"
                )
            },
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


def _source_2432_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2432_STATUS
        and payload.get("harness_ready") is True
        and payload.get("candidate_set_id") == EXPECTED_2432_CANDIDATE_SET_ID
        and payload.get("candidates_tested") == 0
        and payload.get("candidate_gauntlet_run") is False
        and payload.get("recommended_next_research_task") == EXPECTED_2432_NEXT_ROUTE
    )


def _candidate_set_ready(
    candidate_set: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> bool:
    return (
        candidate_set.get("candidate_set_id") == CANDIDATE_SET_ID
        and str(candidate_set.get("status", "")).lower() == "ready"
        and _candidate_set_safety_boundary_ready(candidate_set)
        and bool(rows)
        and all(row["batch_decision"] in DECISION_VALUES for row in rows)
        and not any(row["batch_decision"] == "promotion_candidate" for row in rows)
        and _criteria_thresholds_all_null(candidate_set)
    )


def _candidate_set_safety_boundary_ready(candidate_set: Mapping[str, Any]) -> bool:
    safety = _mapping(candidate_set.get("safety_boundary"))
    return (
        safety.get("research_only") is True
        and safety.get("market_data_screen_allowed_in_2433") is False
        and safety.get("paper_shadow_allowed") is False
        and safety.get("production_allowed") is False
        and safety.get("broker_action") == "none"
        and safety.get("trading_advice_allowed") is False
    )


def _candidate_row(row: Any) -> dict[str, Any]:
    source = _mapping(row)
    questions = [str(item) for item in _sequence(source.get("research_questions"))]
    decision = str(source.get("default_batch_decision", "pit_candidate"))
    return {
        "candidate_id": str(source.get("candidate_id", "")),
        "candidate_family": str(source.get("candidate_family", "")),
        "research_questions": questions,
        "batch_decision": decision,
        "decision_rationale": str(source.get("decision_rationale", "")),
        "next_validation_route": str(source.get("next_validation_route", "")),
        "threshold_source": source.get(
            "threshold_source",
            "future_pit_or_component_validation_policy_required",
        ),
        "threshold_value": source.get("threshold_value"),
        "computed_new_metrics": False,
        "market_data_metrics_available": False,
        "market_data_screen_run": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
    }


def _candidate_screen_matrix(
    rows: Sequence[Mapping[str, Any]],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_SCREEN_MATRIX_SCHEMA_VERSION,
        "status": status,
        "candidate_screen_matrix_ready": not gaps,
        "candidate_count": len(rows),
        "candidates": list(rows),
        "computed_new_metrics": False,
        "market_data_candidate_screen_run": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _batch_decision_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    counts = Counter(row["batch_decision"] for row in rows)
    return {
        "schema_version": BATCH_DECISION_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "batch_decision_summary_ready": not gaps,
        "candidate_count": len(rows),
        "rejected_count": counts["rejected"],
        "component_value_count": counts["component_value"],
        "pit_candidate_count": counts["pit_candidate"],
        "promotion_candidate_count": counts["promotion_candidate"],
        "promotion_candidate_found": counts["promotion_candidate"] > 0,
        "decision_values": list(DECISION_VALUES),
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _research_question_coverage(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    coverage: list[dict[str, Any]] = []
    for question in RESEARCH_QUESTIONS:
        candidate_ids = [
            str(row.get("candidate_id"))
            for row in rows
            if question in _sequence(row.get("research_questions"))
        ]
        coverage.append(
            {
                "research_question_id": question,
                "covered": bool(candidate_ids),
                "candidate_ids": candidate_ids,
            }
        )
    return coverage


def _coverage_section(
    coverage: Sequence[Mapping[str, Any]],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": RESEARCH_QUESTION_COVERAGE_SCHEMA_VERSION,
        "status": status,
        "research_question_coverage_ready": not gaps
        and all(row.get("covered") is True for row in coverage),
        "research_question_count": len(RESEARCH_QUESTIONS),
        "covered_count": sum(1 for row in coverage if row.get("covered") is True),
        "coverage": list(coverage),
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_effect_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": not gaps,
        "candidate_batch_screen_run": status == READY_STATUS,
        "market_data_candidate_screen_run": False,
        "fresh_market_data_read": False,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "computed_new_metrics": False,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "automatic_execution_allowed": False,
        "screen_contract_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _criteria_thresholds_all_null(candidate_set: Mapping[str, Any]) -> bool:
    candidates = [
        item for item in _sequence(candidate_set.get("candidates")) if isinstance(item, Mapping)
    ]
    return bool(candidates) and all(item.get("threshold_value") is None for item in candidates)


def _research_doc_has_harness_reference(research_text: str) -> bool:
    return (
        "growth_tilt_candidate_gauntlet_harness" in research_text
        or "Growth Tilt Candidate Gauntlet Harness" in research_text
    )


def _decision_count(rows: Sequence[Mapping[str, Any]], decision: str) -> int:
    return sum(1 for row in rows if row.get("batch_decision") == decision)


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


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
