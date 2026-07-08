from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_turnover_cooldown_parameter_plateau_study.v1"
PARAMETER_PLATEAU_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_turnover_cooldown_parameter_plateau_matrix.v1"
)
TURNOVER_COOLDOWN_CHECK_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_turnover_cooldown_check_summary.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_turnover_cooldown_parameter_plateau_no_effect_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2435_STATUS = "GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY"
EXPECTED_2435_NEXT_ROUTE = (
    "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study"
)
EXPECTED_2432_STATUS = "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
EXPECTED_CANDIDATE_SET_ID = "growth_tilt_batch_2432"
TURNOVER_COOLDOWN_CANDIDATE_GROUP_ID = "dynamic_turnover_budgeted_growth_tilt_v1"
REQUIRED_PARAMETER_DIMENSIONS: tuple[str, ...] = (
    "risk_off_threshold",
    "turnover_cooldown",
)
REQUIRED_METRIC_IDS: tuple[str, ...] = (
    "turnover_delta_vs_baseline",
    "whipsaw_delta",
    "missed_upside_delta",
    "return_delta_vs_baseline",
    "max_drawdown_delta_vs_baseline",
)
NEXT_ROUTE = "TRADING-2437_Growth_Tilt_Regime_Slice_Attribution_Review"
BLOCKED_ROUTE = (
    "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Evidence_Gap_Remediation"
)
REPORT_TYPE = "growth_tilt_turnover_cooldown_parameter_plateau_study"
CANDIDATE_STATUS_VALUES: tuple[str, ...] = ("component_value", "rejected", "needs_pit")
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_valid_until_outcome_hit_rate_study",
    "growth_tilt_candidate_gauntlet_harness",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-turnover-cooldown-parameter-plateau-study",
    "outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/"
    "parameter_plateau_study_result.json",
    "outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/"
    "parameter_plateau_matrix.json",
    "outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/"
    "turnover_cooldown_check_summary.json",
    "outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_turnover_cooldown_parameter_plateau_study.md",
    "docs/research/growth_tilt_turnover_cooldown_parameter_plateau_matrix.md",
    "docs/research/growth_tilt_turnover_cooldown_check_summary.md",
    "docs/research/growth_tilt_turnover_cooldown_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2437_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-turnover-cooldown-parameter-plateau-study",
    READY_STATUS,
    NEXT_ROUTE,
)
NOT_COMPUTED_DELTA = 0.0
NOT_COMPUTED_COUNT = 0


def build_growth_tilt_turnover_cooldown_parameter_plateau_study(
    source_2435_hit_rate_study: Mapping[str, Any],
    source_2432_candidate_gauntlet: Mapping[str, Any],
    candidate_set_2432: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _requirements(
        source_2435_hit_rate_study,
        source_2432_candidate_gauntlet,
        candidate_set_2432,
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
    matrix = _parameter_plateau_matrix(status, gaps)
    summary = _turnover_cooldown_check_summary(status, gaps)
    boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2436",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2435", "TRADING-2432"],
        "source_2435_ready": _source_2435_ready(source_2435_hit_rate_study),
        "source_2432_gauntlet_ready": _source_2432_ready(source_2432_candidate_gauntlet),
        "candidate_set_parameter_plateau_contract_ready": (
            _candidate_set_parameter_plateau_contract_ready(candidate_set_2432)
        ),
        "candidate_set_turnover_cooldown_group_ready": (
            _candidate_set_turnover_cooldown_group_ready(candidate_set_2432)
        ),
        "candidate_set_required_metrics_ready": (
            _candidate_set_required_metrics_ready(candidate_set_2432)
        ),
        "parameter_plateau_study_ready": status == READY_STATUS,
        "parameter_plateau_matrix_ready": matrix["parameter_plateau_matrix_ready"],
        "turnover_cooldown_check_summary_ready": summary[
            "turnover_cooldown_check_summary_ready"
        ],
        "no_effect_boundary_ready": boundary["no_effect_boundary_ready"],
        "parameter_plateau_found": False,
        "isolated_winner": False,
        "robust_region_count": NOT_COMPUTED_COUNT,
        "component_value_found": False,
        "candidate_status": "needs_pit",
        "candidate_status_values": list(CANDIDATE_STATUS_VALUES),
        "nearby_parameter_pass_count": NOT_COMPUTED_COUNT,
        "turnover_delta": NOT_COMPUTED_DELTA,
        "whipsaw_delta": NOT_COMPUTED_DELTA,
        "missed_upside_delta": NOT_COMPUTED_DELTA,
        "return_degradation": NOT_COMPUTED_DELTA,
        "drawdown_degradation": NOT_COMPUTED_DELTA,
        "computed_new_metrics": False,
        "parameter_sweep_run": False,
        "market_data_parameter_plateau_run": False,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "parameter_plateau_matrix": matrix,
        "turnover_cooldown_check_summary": summary,
        "no_effect_boundary": boundary,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
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
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "recommended_next_research_task": NEXT_ROUTE if status == READY_STATUS else BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Parameter plateau contract is ready but no real sweep was run; continue "
            "to regime slice attribution review."
            if status == READY_STATUS
            else "Required turnover/cooldown plateau study evidence is missing."
        ),
    }


def _requirements(
    source_2435: Mapping[str, Any],
    source_2432: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2435_hit_rate_study_ready",
            _source_2435_ready(source_2435),
            "prior_hit_rate_study_gap",
            {
                "status": source_2435.get("status"),
                "next_route": source_2435.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "source_2432_candidate_gauntlet_ready",
            _source_2432_ready(source_2432),
            "candidate_gauntlet_gap",
            {"status": source_2432.get("status")},
        ),
        _requirement(
            "candidate_set_parameter_plateau_contract_ready",
            _candidate_set_parameter_plateau_contract_ready(candidate_set),
            "candidate_set_parameter_contract_gap",
            {"required_dimensions": list(REQUIRED_PARAMETER_DIMENSIONS)},
        ),
        _requirement(
            "candidate_set_turnover_cooldown_group_ready",
            _candidate_set_turnover_cooldown_group_ready(candidate_set),
            "candidate_set_parameter_contract_gap",
            {"candidate_group_id": TURNOVER_COOLDOWN_CANDIDATE_GROUP_ID},
        ),
        _requirement(
            "candidate_set_required_metrics_ready",
            _candidate_set_required_metrics_ready(candidate_set),
            "candidate_set_metric_gap",
            {"required_metric_ids": list(REQUIRED_METRIC_IDS)},
        ),
        _requirement(
            "prior_research_doc_coverage",
            "turnover" in research_text
            and _candidate_set_parameter_plateau_contract_ready(candidate_set),
            "research_doc_gap",
            {
                "required_references": [
                    "turnover",
                    "candidate_set.parameter_plateau_check.turnover_cooldown",
                ]
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


def _source_2435_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2435_STATUS
        and payload.get("hit_rate_study_ready") is True
        and payload.get("recommended_next_research_task") == EXPECTED_2435_NEXT_ROUTE
    )


def _source_2432_ready(payload: Mapping[str, Any]) -> bool:
    section_status = _mapping(payload.get("candidate_set_section_status"))
    return (
        payload.get("status") == EXPECTED_2432_STATUS
        and payload.get("candidate_set_ready") is True
        and payload.get("candidate_set_id") == EXPECTED_CANDIDATE_SET_ID
        and payload.get("candidates_tested") == 0
        and payload.get("candidate_gauntlet_run") is False
        and section_status.get("parameter_plateau_check") is True
    )


def _candidate_set_parameter_plateau_contract_ready(
    candidate_set: Mapping[str, Any],
) -> bool:
    plateau = _mapping(candidate_set.get("parameter_plateau_check"))
    dimensions = set(str(item) for item in _sequence(plateau.get("dimensions")))
    return (
        candidate_set.get("candidate_set_id") == EXPECTED_CANDIDATE_SET_ID
        and plateau.get("ready") is True
        and set(REQUIRED_PARAMETER_DIMENSIONS).issubset(dimensions)
    )


def _candidate_set_turnover_cooldown_group_ready(
    candidate_set: Mapping[str, Any],
) -> bool:
    groups = _sequence(candidate_set.get("candidate_groups"))
    return any(
        isinstance(group, Mapping)
        and group.get("candidate_group_id") == TURNOVER_COOLDOWN_CANDIDATE_GROUP_ID
        and group.get("default_2431_status") == "component_value"
        and group.get("included_in_2432_harness") is True
        for group in groups
    )


def _candidate_set_required_metrics_ready(candidate_set: Mapping[str, Any]) -> bool:
    metrics = _sequence(_mapping(candidate_set.get("unified_metrics")).get("metrics"))
    metric_ids = {
        str(metric.get("metric_id")) for metric in metrics if isinstance(metric, Mapping)
    }
    return set(REQUIRED_METRIC_IDS).issubset(metric_ids)


def _parameter_plateau_matrix(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": PARAMETER_PLATEAU_MATRIX_SCHEMA_VERSION,
        "status": status,
        "parameter_plateau_matrix_ready": not gaps,
        "parameter_plateau_found": False,
        "isolated_winner": False,
        "robust_region_count": NOT_COMPUTED_COUNT,
        "nearby_parameter_pass_count": NOT_COMPUTED_COUNT,
        "parameter_sweep_run": False,
        "measurement_basis": "not_computed_prior_artifact_contract_only_no_parameter_sweep",
        "production_effect": "none",
        "broker_action": "none",
    }


def _turnover_cooldown_check_summary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": TURNOVER_COOLDOWN_CHECK_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "turnover_cooldown_check_summary_ready": not gaps,
        "turnover_delta": NOT_COMPUTED_DELTA,
        "whipsaw_delta": NOT_COMPUTED_DELTA,
        "missed_upside_delta": NOT_COMPUTED_DELTA,
        "return_degradation": NOT_COMPUTED_DELTA,
        "drawdown_degradation": NOT_COMPUTED_DELTA,
        "computed_new_metrics": False,
        "parameter_sweep_run": False,
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
        "computed_new_metrics": False,
        "parameter_sweep_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
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


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, str):
        return value
    return ()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
