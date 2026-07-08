from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_valid_until_outcome_hit_rate_study.v1"
VALID_UNTIL_HIT_RATE_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_valid_until_outcome_hit_rate_matrix.v1"
)
STALE_SIGNAL_REDUCTION_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_valid_until_stale_signal_reduction_summary.v1"
)
EXPIRY_FAILURE_AUDIT_SCHEMA_VERSION = (
    "growth_tilt_valid_until_expiry_failure_audit.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_valid_until_outcome_hit_rate_no_effect_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2434_STATUS = (
    "GROWTH_TILT_DEFENSIVE_LIMITED_ADJUSTMENT_COMPONENT_VALIDATION_READY"
)
EXPECTED_2434_NEXT_ROUTE = (
    "TRADING-2435_Growth_Tilt_Valid_Until_Outcome_Hit_Rate_Study"
)
EXPECTED_2418_STATUS = (
    "GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY"
)
EXPECTED_2429_STATUS = "GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY_READY"
EXPECTED_CANDIDATE_SET_ID = "growth_tilt_batch_2432"
VALID_UNTIL_METRIC_ID = "valid_until_hit_rate"
VALID_UNTIL_CANDIDATE_GROUP_ID = "dynamic_valid_until_expiry_strict_v1"
NEXT_ROUTE = "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study"
BLOCKED_ROUTE = (
    "TRADING-2435_Growth_Tilt_Valid_Until_Outcome_Hit_Rate_Evidence_Gap_Remediation"
)
REPORT_TYPE = "growth_tilt_valid_until_outcome_hit_rate_study"
CANDIDATE_STATUS_VALUES: tuple[str, ...] = ("component_value", "rejected", "needs_pit")
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_defensive_limited_adjustment_component_validation",
    "growth_tilt_engine_valid_until_dependency_evidence_closure",
    "growth_tilt_engine_forward_outcome_binding_boundary",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-valid-until-outcome-hit-rate-study",
    "outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/"
    "hit_rate_study_result.json",
    "outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/"
    "valid_until_hit_rate_matrix.json",
    "outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/"
    "stale_signal_reduction_summary.json",
    "outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/"
    "expiry_failure_audit.json",
    "outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_valid_until_outcome_hit_rate_study.md",
    "docs/research/growth_tilt_valid_until_hit_rate_matrix.md",
    "docs/research/growth_tilt_valid_until_stale_signal_reduction_summary.md",
    "docs/research/growth_tilt_valid_until_expiry_failure_audit.md",
    "docs/research/growth_tilt_valid_until_outcome_hit_rate_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2436_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-valid-until-outcome-hit-rate-study",
    READY_STATUS,
    NEXT_ROUTE,
)
NO_OBSERVED_OUTCOME_DELTA = 0.0
NO_OBSERVED_EXPIRY_FAILURE_COUNT = 0


def build_growth_tilt_valid_until_outcome_hit_rate_study(
    source_2434_component_validation: Mapping[str, Any],
    source_2418_growth_tilt_valid_until_alignment_evidence: Mapping[str, Any],
    source_2418_stale_signal_policy_evidence: Mapping[str, Any],
    source_2429_forward_outcome_binding_boundary: Mapping[str, Any],
    candidate_set_2432: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _requirements(
        source_2434_component_validation,
        source_2418_growth_tilt_valid_until_alignment_evidence,
        source_2418_stale_signal_policy_evidence,
        source_2429_forward_outcome_binding_boundary,
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
    valid_until_component_value_found = status == READY_STATUS
    candidate_status = "component_value" if valid_until_component_value_found else "needs_pit"
    matrix = _valid_until_hit_rate_matrix(status, gaps)
    stale_summary = _stale_signal_reduction_summary(
        status,
        gaps,
        valid_until_component_value_found=valid_until_component_value_found,
    )
    expiry_audit = _expiry_failure_audit(status, gaps)
    boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2435",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2434", "TRADING-2418", "TRADING-2429", "TRADING-2432"],
        "source_2434_ready": _source_2434_ready(source_2434_component_validation),
        "source_2418_valid_until_evidence_ready": (
            _alignment_evidence_ready(
                source_2418_growth_tilt_valid_until_alignment_evidence
            )
            and _stale_signal_policy_ready(source_2418_stale_signal_policy_evidence)
        ),
        "source_2429_forward_outcome_boundary_ready": (
            _forward_outcome_boundary_ready(source_2429_forward_outcome_binding_boundary)
        ),
        "candidate_set_valid_until_metric_ready": (
            _candidate_set_valid_until_metric_ready(candidate_set_2432)
        ),
        "candidate_set_valid_until_candidate_group_ready": (
            _candidate_set_valid_until_candidate_group_ready(candidate_set_2432)
        ),
        "hit_rate_study_ready": status == READY_STATUS,
        "valid_until_hit_rate_matrix_ready": matrix["valid_until_hit_rate_matrix_ready"],
        "stale_signal_reduction_summary_ready": stale_summary[
            "stale_signal_reduction_summary_ready"
        ],
        "expiry_failure_audit_ready": expiry_audit["expiry_failure_audit_ready"],
        "no_effect_boundary_ready": boundary["no_effect_boundary_ready"],
        "valid_until_component_value_found": valid_until_component_value_found,
        "component_value_found": valid_until_component_value_found,
        "valid_until_hit_rate_delta": matrix["valid_until_hit_rate_delta"],
        "stale_signal_reduction": stale_summary["stale_signal_reduction"],
        "expiry_failure_count": expiry_audit["expiry_failure_count"],
        "candidate_status": candidate_status,
        "candidate_status_values": list(CANDIDATE_STATUS_VALUES),
        "outcome_sample_count": 0,
        "observed_outcome_hit_rate_available": False,
        "computed_new_metrics": False,
        "market_data_hit_rate_study_run": False,
        "real_outcome_binding_run": False,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "valid_until_hit_rate_matrix": matrix,
        "stale_signal_reduction_summary": stale_summary,
        "expiry_failure_audit": expiry_audit,
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
            "Valid-until contract evidence supports component value; continue to "
            "turnover/cooldown parameter plateau study."
            if status == READY_STATUS
            else "Required valid-until outcome hit-rate study evidence is missing."
        ),
    }


def _requirements(
    source_2434: Mapping[str, Any],
    alignment_evidence: Mapping[str, Any],
    stale_signal_policy_evidence: Mapping[str, Any],
    forward_outcome_boundary: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2434_component_validation_ready",
            _source_2434_ready(source_2434),
            "prior_component_validation_gap",
            {
                "status": source_2434.get("status"),
                "next_route": source_2434.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "source_2418_valid_until_alignment_ready",
            _alignment_evidence_ready(alignment_evidence),
            "valid_until_evidence_gap",
            {"status": alignment_evidence.get("status")},
        ),
        _requirement(
            "source_2418_stale_signal_policy_ready",
            _stale_signal_policy_ready(stale_signal_policy_evidence),
            "valid_until_evidence_gap",
            {"status": stale_signal_policy_evidence.get("status")},
        ),
        _requirement(
            "source_2429_forward_outcome_boundary_ready",
            _forward_outcome_boundary_ready(forward_outcome_boundary),
            "forward_outcome_boundary_gap",
            {"status": forward_outcome_boundary.get("status")},
        ),
        _requirement(
            "candidate_set_valid_until_metric_ready",
            _candidate_set_valid_until_metric_ready(candidate_set),
            "candidate_set_metric_gap",
            {"metric_id": VALID_UNTIL_METRIC_ID},
        ),
        _requirement(
            "candidate_set_valid_until_candidate_group_ready",
            _candidate_set_valid_until_candidate_group_ready(candidate_set),
            "candidate_set_metric_gap",
            {"candidate_group_id": VALID_UNTIL_CANDIDATE_GROUP_ID},
        ),
        _requirement(
            "prior_research_doc_coverage",
            "valid_until" in research_text and "outcome" in research_text,
            "research_doc_gap",
            {"required_references": ["valid_until", "outcome"]},
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


def _source_2434_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2434_STATUS
        and payload.get("component_validation_ready") is True
        and payload.get("valid_until_component_value_found") is not False
        and payload.get("recommended_next_research_task") == EXPECTED_2434_NEXT_ROUTE
    )


def _alignment_evidence_ready(payload: Mapping[str, Any]) -> bool:
    evidence = _mapping(payload.get("growth_tilt_valid_until_alignment_evidence"))
    return (
        payload.get("status") == EXPECTED_2418_STATUS
        and evidence.get("ready_for_recheck") is True
        and isinstance(evidence.get("proposed_horizon_to_valid_until_mapping"), Sequence)
        and not isinstance(evidence.get("proposed_horizon_to_valid_until_mapping"), str)
    )


def _stale_signal_policy_ready(payload: Mapping[str, Any]) -> bool:
    evidence = _mapping(payload.get("stale_signal_policy_evidence"))
    return (
        payload.get("status") == EXPECTED_2418_STATUS
        and evidence.get("ready_for_recheck") is True
        and evidence.get("stale_carry_forward_policy_ready") is True
        and evidence.get("signal_to_execution_lag_policy_ready") is True
        and evidence.get("replay_validation_required") is True
    )


def _forward_outcome_boundary_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2429_STATUS
        and payload.get("forward_outcome_binding_boundary_ready") is True
        and payload.get("valid_until_binding_ready") is True
        and payload.get("outcome_schema_ready") is True
        and payload.get("signal_to_outcome_linkage_ready") is True
    )


def _candidate_set_valid_until_metric_ready(candidate_set: Mapping[str, Any]) -> bool:
    if candidate_set.get("candidate_set_id") != EXPECTED_CANDIDATE_SET_ID:
        return False
    metrics = _sequence(_mapping(candidate_set.get("unified_metrics")).get("metrics"))
    return any(
        isinstance(metric, Mapping) and metric.get("metric_id") == VALID_UNTIL_METRIC_ID
        for metric in metrics
    )


def _candidate_set_valid_until_candidate_group_ready(
    candidate_set: Mapping[str, Any],
) -> bool:
    groups = _sequence(candidate_set.get("candidate_groups"))
    return any(
        isinstance(group, Mapping)
        and group.get("candidate_group_id") == VALID_UNTIL_CANDIDATE_GROUP_ID
        and group.get("default_2431_status") == "component_value"
        and group.get("included_in_2432_harness") is True
        for group in groups
    )


def _valid_until_hit_rate_matrix(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": VALID_UNTIL_HIT_RATE_MATRIX_SCHEMA_VERSION,
        "status": status,
        "valid_until_hit_rate_matrix_ready": not gaps,
        "outcome_sample_count": 0,
        "observed_outcome_hit_rate_available": False,
        "baseline_hit_rate": None,
        "valid_until_hit_rate": None,
        "valid_until_hit_rate_delta": NO_OBSERVED_OUTCOME_DELTA,
        "computed_new_metrics": False,
        "measurement_basis": (
            "not_computed_prior_artifact_contract_only_no_real_outcome_binding"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _stale_signal_reduction_summary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
    *,
    valid_until_component_value_found: bool,
) -> dict[str, Any]:
    return {
        "schema_version": STALE_SIGNAL_REDUCTION_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "stale_signal_reduction_summary_ready": not gaps,
        "valid_until_component_value_found": valid_until_component_value_found,
        "stale_signal_reduction": NO_OBSERVED_OUTCOME_DELTA,
        "stale_signal_reduction_basis": (
            "contract_evidence_only_no_fresh_outcome_sample"
        ),
        "computed_new_metrics": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _expiry_failure_audit(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": EXPIRY_FAILURE_AUDIT_SCHEMA_VERSION,
        "status": status,
        "expiry_failure_audit_ready": not gaps,
        "expiry_failure_count": NO_OBSERVED_EXPIRY_FAILURE_COUNT,
        "expiry_failure_count_basis": "no_real_outcome_binding_executed_in_2435",
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
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
        "outcome_store_mutated": False,
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
