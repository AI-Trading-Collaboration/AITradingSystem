from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_candidate_gauntlet_harness.v1"
CANDIDATE_SET_SNAPSHOT_SCHEMA_VERSION = (
    "growth_tilt_candidate_gauntlet_candidate_set_snapshot.v1"
)
BASELINE_CONTRACT_SCHEMA_VERSION = (
    "growth_tilt_candidate_gauntlet_baseline_contract.v1"
)
METRIC_CONTRACT_SCHEMA_VERSION = "growth_tilt_candidate_gauntlet_metric_contract.v1"
CRITERIA_CONTRACT_SCHEMA_VERSION = "growth_tilt_candidate_gauntlet_criteria_contract.v1"
REGIME_PLATEAU_ABLATION_SCHEMA_VERSION = (
    "growth_tilt_candidate_gauntlet_regime_plateau_ablation_contract.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = "growth_tilt_candidate_gauntlet_no_effect_boundary.v1"

READY_STATUS = "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
BLOCKED_STATUS = "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_BLOCKED_BY_CONTRACT_GAPS"
EXPECTED_2431_STATUS = "GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY"
EXPECTED_2431_NEXT_ROUTE = "TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness"
NEXT_ROUTE = "TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen"
BLOCKED_ROUTE = "TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness_Gap_Remediation"
REPORT_TYPE = "growth_tilt_candidate_gauntlet_harness"
CANDIDATE_SET_ID = "growth_tilt_batch_2432"

REQUIRED_METRICS: tuple[str, ...] = (
    "return_delta_vs_baseline",
    "sharpe_delta_vs_baseline",
    "max_drawdown_delta_vs_baseline",
    "turnover_delta_vs_baseline",
    "false_risk_off_delta",
    "missed_upside_delta",
    "whipsaw_delta",
    "valid_until_hit_rate",
    "parameter_robustness_score",
    "regime_robustness_score",
    "forward_aging_score",
)
REQUIRED_CANDIDATE_SET_SECTIONS: tuple[str, ...] = (
    "batch_runner",
    "unified_baseline",
    "unified_metrics",
    "kill_criteria",
    "promotion_criteria",
    "parameter_plateau_check",
    "regime_slice_check",
    "ablation_output",
    "safety_boundary",
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_existing_candidate_evidence_matrix",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "research/configs/growth_tilt/candidate_set_2432.yaml",
    "aits research strategies growth-tilt-candidate-gauntlet",
    "outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/"
    "candidate_gauntlet_result.json",
    "outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/"
    "candidate_set_snapshot.json",
    "outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/"
    "gauntlet_baseline_contract.json",
    "outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/"
    "gauntlet_metric_contract.json",
    "outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/"
    "gauntlet_criteria_contract.json",
    "outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/"
    "regime_plateau_ablation_contract.json",
    "outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_candidate_gauntlet_harness.md",
    "docs/research/growth_tilt_candidate_set_2432.md",
    "docs/research/growth_tilt_candidate_gauntlet_baseline_contract.md",
    "docs/research/growth_tilt_candidate_gauntlet_metric_contract.md",
    "docs/research/growth_tilt_candidate_gauntlet_criteria_contract.md",
    "docs/research/growth_tilt_candidate_gauntlet_regime_plateau_ablation_contract.md",
    "docs/research/growth_tilt_candidate_gauntlet_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2433_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-candidate-gauntlet",
    CANDIDATE_SET_ID,
    READY_STATUS,
    NEXT_ROUTE,
)


def build_growth_tilt_candidate_gauntlet_harness(
    source_2431_existing_candidate_evidence_matrix: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    section_status = _candidate_set_section_status(candidate_set)
    candidate_groups = _candidate_groups(candidate_set)
    requirements = _requirements(
        source_2431_existing_candidate_evidence_matrix,
        candidate_set,
        section_status,
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

    candidate_set_snapshot = _candidate_set_snapshot(
        candidate_set,
        candidate_groups,
        status=status,
        gaps=gaps,
    )
    baseline_contract = _baseline_contract(candidate_set, status=status, gaps=gaps)
    metric_contract = _metric_contract(candidate_set, status=status, gaps=gaps)
    criteria_contract = _criteria_contract(candidate_set, status=status, gaps=gaps)
    regime_plateau_ablation_contract = _regime_plateau_ablation_contract(
        candidate_set,
        status=status,
        gaps=gaps,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2432",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2431"],
        "source_2431_ready": _source_2431_ready(
            source_2431_existing_candidate_evidence_matrix
        ),
        "candidate_set_ready": _candidate_set_ready(candidate_set, section_status),
        "candidate_set_id": str(candidate_set.get("candidate_set_id", "")),
        "candidate_set_schema_version": candidate_set.get("schema_version"),
        "candidate_set_section_status": section_status,
        "growth_tilt_candidate_gauntlet_result": {
            "candidate_set_id": str(candidate_set.get("candidate_set_id", "")),
            "candidates_tested": 0,
            "harness_ready": not gaps,
            "baseline_ready": baseline_contract["baseline_ready"],
            "metrics_ready": metric_contract["metrics_ready"],
            "kill_criteria_ready": criteria_contract["kill_criteria_ready"],
            "promotion_criteria_ready": criteria_contract["promotion_criteria_ready"],
            "regime_slices_ready": (
                regime_plateau_ablation_contract["regime_slices_ready"]
            ),
            "parameter_plateau_check_ready": (
                regime_plateau_ablation_contract["parameter_plateau_check_ready"]
            ),
            "ablation_output_ready": (
                regime_plateau_ablation_contract["ablation_output_ready"]
            ),
            "next_route": next_route,
        },
        "harness_ready": not gaps,
        "baseline_ready": baseline_contract["baseline_ready"],
        "metrics_ready": metric_contract["metrics_ready"],
        "kill_criteria_ready": criteria_contract["kill_criteria_ready"],
        "promotion_criteria_ready": criteria_contract["promotion_criteria_ready"],
        "regime_slices_ready": regime_plateau_ablation_contract[
            "regime_slices_ready"
        ],
        "parameter_plateau_check_ready": regime_plateau_ablation_contract[
            "parameter_plateau_check_ready"
        ],
        "ablation_output_ready": regime_plateau_ablation_contract[
            "ablation_output_ready"
        ],
        "candidate_group_count": len(candidate_groups),
        "candidates_tested": 0,
        "required_metric_count": len(REQUIRED_METRICS),
        "configured_metric_count": len(_configured_metric_ids(candidate_set)),
        "kill_criteria_count": len(_sequence(candidate_set.get("kill_criteria"))),
        "promotion_criteria_count": len(
            _sequence(candidate_set.get("promotion_criteria"))
        ),
        "regime_slice_count": len(
            _sequence(_mapping(candidate_set.get("regime_slice_check")).get("slices"))
        ),
        "parameter_plateau_dimension_count": len(
            _sequence(
                _mapping(candidate_set.get("parameter_plateau_check")).get("dimensions")
            )
        ),
        "ablation_output_count": len(
            _sequence(_mapping(candidate_set.get("ablation_output")).get("outputs"))
        ),
        "new_investment_threshold_values_set": False,
        "threshold_policy_required_for_execution": True,
        "criteria_threshold_values_all_null": _criteria_thresholds_all_null(
            candidate_set
        ),
        "requirements": requirements,
        "gaps": gaps,
        "contract_gap_count": len(gaps),
        "contract_gap_ids": [gap["requirement_id"] for gap in gaps],
        "candidate_set_snapshot": candidate_set_snapshot,
        "baseline_contract": baseline_contract,
        "metric_contract": metric_contract,
        "criteria_contract": criteria_contract,
        "regime_plateau_ablation_contract": regime_plateau_ablation_contract,
        "no_effect_boundary": no_effect_boundary,
        "candidate_gauntlet_harness_ready": not gaps,
        "candidate_gauntlet_run": False,
        "candidate_batch_screen_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "market_data_experiment_run": False,
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
            "Candidate gauntlet harness is ready; continue to the false risk-off "
            "and missed-upside batch screen."
            if status == READY_STATUS
            else "Required harness contract, prior evidence, or documentation coverage is missing."
        ),
    }


def _requirements(
    source_2431: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    section_status: Mapping[str, bool],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2431_existing_candidate_evidence_matrix_ready",
            _source_2431_ready(source_2431),
            "prior_evidence_gap",
            {
                "status": source_2431.get("status"),
                "next_route": source_2431.get("recommended_next_research_task"),
                "candidate_count": source_2431.get("candidate_count"),
            },
        ),
        _requirement(
            "candidate_set_id_ready",
            candidate_set.get("candidate_set_id") == CANDIDATE_SET_ID,
            "candidate_set_gap",
            {"candidate_set_id": candidate_set.get("candidate_set_id")},
        ),
        _requirement(
            "candidate_set_sections_ready",
            all(section_status.get(section) is True for section in REQUIRED_CANDIDATE_SET_SECTIONS),
            "candidate_set_gap",
            dict(section_status),
        ),
        _requirement(
            "candidate_set_safety_boundary",
            _candidate_set_safety_boundary_ready(candidate_set),
            "safety_boundary_gap",
            dict(_mapping(candidate_set.get("safety_boundary"))),
        ),
        _requirement(
            "candidate_set_metric_coverage",
            set(REQUIRED_METRICS).issubset(_configured_metric_ids(candidate_set)),
            "metric_contract_gap",
            {
                "required_metrics": list(REQUIRED_METRICS),
                "configured_metrics": sorted(_configured_metric_ids(candidate_set)),
            },
        ),
        _requirement(
            "criteria_threshold_governance",
            _criteria_thresholds_all_null(candidate_set),
            "heuristic_governance_gap",
            {
                "new_investment_threshold_values_set": False,
                "threshold_policy_required_for_execution": True,
            },
        ),
        _requirement(
            "prior_research_doc_coverage",
            "growth_tilt_existing_candidate_evidence_matrix" in research_text,
            "research_doc_gap",
            {"required_reference": "growth_tilt_existing_candidate_evidence_matrix"},
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


def _source_2431_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2431_STATUS
        and payload.get("existing_candidate_evidence_matrix_ready") is True
        and payload.get("candidate_count") == 6
        and payload.get("promotion_candidate_found") is False
        and payload.get("candidate_gauntlet_run") is False
        and payload.get("recommended_next_research_task") == EXPECTED_2431_NEXT_ROUTE
    )


def _candidate_set_ready(
    candidate_set: Mapping[str, Any],
    section_status: Mapping[str, bool],
) -> bool:
    return (
        candidate_set.get("candidate_set_id") == CANDIDATE_SET_ID
        and str(candidate_set.get("status", "")).lower() == "ready"
        and all(section_status.get(section) is True for section in REQUIRED_CANDIDATE_SET_SECTIONS)
        and _candidate_set_safety_boundary_ready(candidate_set)
        and set(REQUIRED_METRICS).issubset(_configured_metric_ids(candidate_set))
        and _criteria_thresholds_all_null(candidate_set)
    )


def _candidate_set_section_status(candidate_set: Mapping[str, Any]) -> dict[str, bool]:
    baseline = _mapping(candidate_set.get("unified_baseline"))
    metrics = _mapping(candidate_set.get("unified_metrics"))
    plateau = _mapping(candidate_set.get("parameter_plateau_check"))
    regime = _mapping(candidate_set.get("regime_slice_check"))
    ablation = _mapping(candidate_set.get("ablation_output"))
    return {
        "batch_runner": bool(_mapping(candidate_set.get("batch_runner")).get("runner_id")),
        "unified_baseline": bool(baseline.get("baseline_id")),
        "unified_metrics": bool(_sequence(metrics.get("metrics"))),
        "kill_criteria": bool(_sequence(candidate_set.get("kill_criteria"))),
        "promotion_criteria": bool(_sequence(candidate_set.get("promotion_criteria"))),
        "parameter_plateau_check": bool(
            plateau.get("ready") is True and _sequence(plateau.get("dimensions"))
        ),
        "regime_slice_check": bool(
            regime.get("ready") is True and _sequence(regime.get("slices"))
        ),
        "ablation_output": bool(
            ablation.get("ready") is True and _sequence(ablation.get("outputs"))
        ),
        "safety_boundary": _candidate_set_safety_boundary_ready(candidate_set),
    }


def _candidate_set_safety_boundary_ready(candidate_set: Mapping[str, Any]) -> bool:
    safety = _mapping(candidate_set.get("safety_boundary"))
    return (
        safety.get("research_only") is True
        and safety.get("candidate_execution_allowed") is False
        and safety.get("candidate_gauntlet_run_allowed_in_2432") is False
        and safety.get("paper_shadow_allowed") is False
        and safety.get("production_allowed") is False
        and safety.get("broker_action") == "none"
        and safety.get("trading_advice_allowed") is False
    )


def _candidate_groups(candidate_set: Mapping[str, Any]) -> list[dict[str, Any]]:
    groups = []
    for row in _sequence(candidate_set.get("candidate_groups")):
        if not isinstance(row, Mapping):
            continue
        groups.append(
            {
                "candidate_group_id": str(row.get("candidate_group_id", "")),
                "candidate_family": str(row.get("candidate_family", "")),
                "source_candidate_ids": [
                    str(item) for item in _sequence(row.get("source_candidate_ids"))
                ],
                "default_2431_status": row.get("default_2431_status"),
                "included_in_2432_harness": row.get("included_in_2432_harness") is True,
                "execution_status": "not_executed_in_2432",
            }
        )
    return groups


def _candidate_set_snapshot(
    candidate_set: Mapping[str, Any],
    candidate_groups: Sequence[Mapping[str, Any]],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_SET_SNAPSHOT_SCHEMA_VERSION,
        "status": status,
        "candidate_set_ready": not gaps,
        "candidate_set_id": candidate_set.get("candidate_set_id"),
        "candidate_group_count": len(candidate_groups),
        "candidates_tested": 0,
        "candidate_groups": list(candidate_groups),
        "production_effect": "none",
        "broker_action": "none",
    }


def _baseline_contract(
    candidate_set: Mapping[str, Any],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    baseline = dict(_mapping(candidate_set.get("unified_baseline")))
    return {
        "schema_version": BASELINE_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "baseline_ready": not gaps and bool(baseline.get("baseline_id")),
        "unified_baseline": baseline,
        "computed_in_2432": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _metric_contract(
    candidate_set: Mapping[str, Any],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    metrics = [
        dict(item)
        for item in _sequence(
            _mapping(candidate_set.get("unified_metrics")).get("metrics")
        )
        if isinstance(item, Mapping)
    ]
    configured = {str(item.get("metric_id")) for item in metrics}
    return {
        "schema_version": METRIC_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "metrics_ready": not gaps and set(REQUIRED_METRICS).issubset(configured),
        "required_metrics": list(REQUIRED_METRICS),
        "metrics": metrics,
        "computed_in_2432": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _criteria_contract(
    candidate_set: Mapping[str, Any],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    kill = [
        dict(item)
        for item in _sequence(candidate_set.get("kill_criteria"))
        if isinstance(item, Mapping)
    ]
    promotion = [
        dict(item)
        for item in _sequence(candidate_set.get("promotion_criteria"))
        if isinstance(item, Mapping)
    ]
    thresholds_null = _criteria_thresholds_all_null(candidate_set)
    return {
        "schema_version": CRITERIA_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "kill_criteria_ready": not gaps and bool(kill),
        "promotion_criteria_ready": not gaps and bool(promotion),
        "kill_criteria": kill,
        "promotion_criteria": promotion,
        "new_investment_threshold_values_set": False,
        "threshold_policy_required_for_execution": True,
        "criteria_threshold_values_all_null": thresholds_null,
        "computed_in_2432": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _regime_plateau_ablation_contract(
    candidate_set: Mapping[str, Any],
    *,
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    plateau = dict(_mapping(candidate_set.get("parameter_plateau_check")))
    regime = dict(_mapping(candidate_set.get("regime_slice_check")))
    ablation = dict(_mapping(candidate_set.get("ablation_output")))
    return {
        "schema_version": REGIME_PLATEAU_ABLATION_SCHEMA_VERSION,
        "status": status,
        "parameter_plateau_check_ready": not gaps
        and plateau.get("ready") is True
        and bool(_sequence(plateau.get("dimensions"))),
        "regime_slices_ready": not gaps
        and regime.get("ready") is True
        and bool(_sequence(regime.get("slices"))),
        "ablation_output_ready": not gaps
        and ablation.get("ready") is True
        and bool(_sequence(ablation.get("outputs"))),
        "parameter_plateau_check": plateau,
        "regime_slice_check": regime,
        "ablation_output": ablation,
        "computed_in_2432": False,
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
        "candidate_gauntlet_harness_ready": status == READY_STATUS,
        "candidate_gauntlet_run": False,
        "candidate_batch_screen_run": False,
        "candidates_tested": 0,
        "fresh_market_data_read": False,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
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
        "contract_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _configured_metric_ids(candidate_set: Mapping[str, Any]) -> set[str]:
    metrics = _sequence(_mapping(candidate_set.get("unified_metrics")).get("metrics"))
    return {
        str(item.get("metric_id"))
        for item in metrics
        if isinstance(item, Mapping) and item.get("metric_id")
    }


def _criteria_thresholds_all_null(candidate_set: Mapping[str, Any]) -> bool:
    criteria = [
        item
        for item in (
            _sequence(candidate_set.get("kill_criteria"))
            + _sequence(candidate_set.get("promotion_criteria"))
        )
        if isinstance(item, Mapping)
    ]
    plateau = _mapping(candidate_set.get("parameter_plateau_check"))
    criteria_threshold_values = [item.get("threshold_value") for item in criteria]
    criteria_threshold_values.append(plateau.get("threshold_value"))
    return bool(criteria) and all(value is None for value in criteria_threshold_values)


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
