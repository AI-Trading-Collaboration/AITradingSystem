from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_existing_candidate_evidence_matrix.v1"
CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_existing_candidate_evidence_matrix_table.v1"
)
CANDIDATE_STATUS_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_existing_candidate_status_summary.v1"
)
CANDIDATE_METRIC_COVERAGE_SCHEMA_VERSION = (
    "growth_tilt_existing_candidate_metric_coverage.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_existing_candidate_evidence_matrix_no_effect_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2430_STATUS = (
    "GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_NO_PROMOTION_CANDIDATE"
)
EXPECTED_2430_NEXT_ROUTE = (
    "TRADING-2431_Growth_Tilt_Existing_Candidate_Evidence_Matrix"
)
NEXT_ROUTE = "TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness"
BLOCKED_ROUTE = "TRADING-2432_Growth_Tilt_Existing_Candidate_Evidence_Gap_Remediation"
REPORT_TYPE = "growth_tilt_existing_candidate_evidence_matrix"

CANDIDATE_STATUS_VALUES: tuple[str, ...] = (
    "rejected",
    "component_value",
    "needs_pit",
    "promotion_candidate",
)
REQUIRED_CANDIDATE_GROUPS: tuple[dict[str, Any], ...] = (
    {
        "candidate_group_id": "defensive_limited_adjustment",
        "candidate_family": "defensive_limited_adjustment",
        "source_candidate_ids": ["defensive_limited_adjustment"],
        "default_status": "component_value",
        "primary_value": "drawdown_control_research_hypothesis",
        "next_validation_route": (
            "TRADING-2434_Defensive_Limited_Adjustment_Component_Validation"
        ),
    },
    {
        "candidate_group_id": "lower_turnover_variants",
        "candidate_family": "lower_turnover_guardrail",
        "source_candidate_ids": [
            "dynamic_regime_overlay_v0_4_lower_turnover",
            "dynamic_regime_growth_tilt_lower_turnover_fusion_v1",
            "equal_risk_growth_tilt_lower_turnover_guarded_v1",
            "growth_tilt_lower_turnover_guarded_transfer_v1",
        ],
        "default_status": "component_value",
        "primary_value": "turnover_and_whipsaw_control",
        "next_validation_route": (
            "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study"
        ),
    },
    {
        "candidate_group_id": "dynamic_valid_until_expiry_strict_v1",
        "candidate_family": "valid_until_strictness",
        "source_candidate_ids": ["dynamic_valid_until_expiry_strict_v1"],
        "default_status": "component_value",
        "primary_value": "stale_signal_reduction",
        "next_validation_route": (
            "TRADING-2435_Growth_Tilt_Valid_Until_Outcome_Hit_Rate_Study"
        ),
    },
    {
        "candidate_group_id": "dynamic_turnover_budgeted_growth_tilt_v1",
        "candidate_family": "turnover_budgeting",
        "source_candidate_ids": ["dynamic_turnover_budgeted_growth_tilt_v1"],
        "default_status": "component_value",
        "primary_value": "turnover_budget_discipline",
        "next_validation_route": (
            "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study"
        ),
    },
    {
        "candidate_group_id": "equal_risk_growth_tilt_vol_target_variants",
        "candidate_family": "vol_target_growth_tilt",
        "source_candidate_ids": [
            "equal_risk_growth_tilt_vol_target_v1",
            "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
        ],
        "default_status": "needs_pit",
        "primary_value": "return_gap_repair_candidate",
        "next_validation_route": "TRADING-2438_Growth_Tilt_Top-3_Candidate_PIT_Replay",
    },
    {
        "candidate_group_id": "growth_tilt_engine_signal_variants",
        "candidate_family": "growth_tilt_engine_signal",
        "source_candidate_ids": [
            "growth_tilt_engine_signal",
            "growth_tilt_engine_signal_artifact",
        ],
        "default_status": "needs_pit",
        "primary_value": "observe_only_signal_candidate_family",
        "next_validation_route": "TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness",
    },
)
RECOMMENDED_METRICS: tuple[str, ...] = (
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
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_candidate_promotion_evidence_review",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-existing-candidate-evidence-matrix",
    "outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/"
    "existing_candidate_evidence_matrix_result.json",
    "outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/"
    "candidate_evidence_matrix.json",
    "outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/"
    "candidate_status_summary.json",
    "outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/"
    "candidate_metric_coverage.json",
    "outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_existing_candidate_evidence_matrix.md",
    "docs/research/growth_tilt_existing_candidate_evidence_matrix_table.md",
    "docs/research/growth_tilt_existing_candidate_status_summary.md",
    "docs/research/growth_tilt_existing_candidate_metric_coverage.md",
    "docs/research/growth_tilt_existing_candidate_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2432_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-existing-candidate-evidence-matrix",
    READY_STATUS,
    NEXT_ROUTE,
)


def build_growth_tilt_existing_candidate_evidence_matrix(
    source_2430_promotion_review: Mapping[str, Any],
    candidate_registry: Mapping[str, Any],
    prior_candidate_evidence: Mapping[str, Any],
    prior_component_value_matrix: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    rows = [
        _candidate_row(
            group,
            source_2430_promotion_review,
            candidate_registry,
            prior_candidate_evidence,
            prior_component_value_matrix,
            research_text,
        )
        for group in REQUIRED_CANDIDATE_GROUPS
    ]
    requirements = _requirements(
        source_2430_promotion_review,
        candidate_registry,
        prior_candidate_evidence,
        prior_component_value_matrix,
        rows,
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
    status_counts = Counter(row["candidate_status"] for row in rows)
    metric_coverage = _metric_coverage(rows)
    summary = {
        "schema_version": CANDIDATE_STATUS_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "candidate_status_summary_ready": not gaps,
        "candidate_count": len(rows),
        "rejected_count": status_counts["rejected"],
        "component_value_count": status_counts["component_value"],
        "needs_pit_count": status_counts["needs_pit"],
        "promotion_candidate_count": status_counts["promotion_candidate"],
        "promotion_candidate_found": status_counts["promotion_candidate"] > 0,
        "status_values": list(CANDIDATE_STATUS_VALUES),
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }
    no_effect_boundary = _no_effect_boundary(status, gaps)
    matrix = {
        "schema_version": CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION,
        "status": status,
        "candidate_evidence_matrix_ready": not gaps and len(rows) == (
            len(REQUIRED_CANDIDATE_GROUPS)
        ),
        "candidate_count": len(rows),
        "candidates": rows,
        "production_effect": "none",
        "broker_action": "none",
    }

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2431",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2430", "TRADING-2390", "TRADING-2391", "TRADING-2392"],
        "source_2430_ready": _source_2430_ready(source_2430_promotion_review),
        "candidate_registry_ready": _candidate_registry_ready(candidate_registry),
        "prior_candidate_evidence_ready": _prior_candidate_evidence_ready(
            prior_candidate_evidence
        ),
        "component_value_evidence_ready": _component_value_evidence_ready(
            prior_component_value_matrix
        ),
        "existing_candidate_evidence_matrix_ready": matrix[
            "candidate_evidence_matrix_ready"
        ],
        "candidate_status_summary_ready": summary["candidate_status_summary_ready"],
        "candidate_metric_coverage_ready": metric_coverage[
            "candidate_metric_coverage_ready"
        ],
        "no_effect_boundary_ready": no_effect_boundary["no_effect_boundary_ready"],
        "candidate_count": len(rows),
        "required_candidate_group_count": len(REQUIRED_CANDIDATE_GROUPS),
        "rejected_count": status_counts["rejected"],
        "component_value_count": status_counts["component_value"],
        "needs_pit_count": status_counts["needs_pit"],
        "promotion_candidate_count": status_counts["promotion_candidate"],
        "promotion_candidate_found": status_counts["promotion_candidate"] > 0,
        "metric_coverage_available_count": metric_coverage[
            "metric_coverage_available_count"
        ],
        "metric_coverage_partial_count": metric_coverage[
            "metric_coverage_partial_count"
        ],
        "metric_coverage_missing_count": metric_coverage[
            "metric_coverage_missing_count"
        ],
        "engineering_readiness_is_alpha_evidence": False,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "candidate_gauntlet_run": False,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "candidate_evidence_matrix": matrix,
        "candidate_status_summary": summary,
        "candidate_metric_coverage": metric_coverage,
        "no_effect_boundary": no_effect_boundary,
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
            "Existing candidates are organized; continue to the batch gauntlet "
            "harness before expensive PIT replay or paper-shadow review."
            if status == READY_STATUS
            else "Required existing candidate evidence is missing or incomplete."
        ),
    }


def _candidate_row(
    group: Mapping[str, Any],
    source_2430: Mapping[str, Any],
    registry: Mapping[str, Any],
    prior_evidence: Mapping[str, Any],
    component_matrix: Mapping[str, Any],
    research_text: str,
) -> dict[str, Any]:
    candidate_ids = [str(item) for item in group.get("source_candidate_ids", [])]
    promotion_ids = _promotion_candidate_ids(source_2430)
    is_promotion_candidate = any(candidate_id in promotion_ids for candidate_id in candidate_ids)
    component_value_ids = _component_value_ids(prior_evidence, component_matrix)
    registry_ids = _registry_candidate_ids(registry)
    evidence_refs = _evidence_references(candidate_ids, research_text)
    status = (
        "promotion_candidate"
        if is_promotion_candidate
        else "component_value"
        if (
            group.get("default_status") == "component_value"
            or any(candidate_id in component_value_ids for candidate_id in candidate_ids)
        )
        else str(group.get("default_status", "needs_pit"))
    )
    metric_status = _metric_status_for_group(
        str(group["candidate_group_id"]),
        prior_evidence,
        evidence_refs,
    )
    blockers = _candidate_blockers(status, source_2430, prior_evidence, evidence_refs)
    return {
        "candidate_group_id": group["candidate_group_id"],
        "candidate_family": group["candidate_family"],
        "source_candidate_ids": candidate_ids,
        "candidate_status": status,
        "status_rationale": _status_rationale(status, group, prior_evidence),
        "primary_value": group["primary_value"],
        "next_validation_route": group["next_validation_route"],
        "registry_candidate_overlap": [
            candidate_id for candidate_id in candidate_ids if candidate_id in registry_ids
        ],
        "component_value_evidence_present": any(
            candidate_id in component_value_ids for candidate_id in candidate_ids
        ),
        "prior_doc_evidence_present": bool(evidence_refs),
        "prior_evidence_references": evidence_refs,
        "metric_coverage_status": metric_status["coverage_status"],
        "metric_coverage": metric_status["metrics"],
        "known_blockers": blockers,
        "paper_shadow_promotion_candidate": is_promotion_candidate,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
    }


def _requirements(
    source_2430: Mapping[str, Any],
    registry: Mapping[str, Any],
    prior_evidence: Mapping[str, Any],
    component_matrix: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2430_no_promotion_review_ready",
            _source_2430_ready(source_2430),
            "prior_promotion_review_gap",
            {
                "status": source_2430.get("status"),
                "next_route": source_2430.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "candidate_registry_ready",
            _candidate_registry_ready(registry),
            "candidate_registry_gap",
            {"policy_id": registry.get("policy_id")},
        ),
        _requirement(
            "prior_candidate_evidence_ready",
            _prior_candidate_evidence_ready(prior_evidence),
            "prior_candidate_evidence_gap",
            {
                "current_best_candidate": prior_evidence.get("current_best_candidate"),
                "owner_decision": prior_evidence.get("owner_decision"),
            },
        ),
        _requirement(
            "component_value_evidence_ready",
            _component_value_evidence_ready(component_matrix),
            "component_value_evidence_gap",
            {
                "schema_version": component_matrix.get("schema_version"),
                "component_value_candidates": component_matrix.get(
                    "component_value_candidates"
                ),
            },
        ),
        _requirement(
            "required_candidate_group_coverage",
            len(rows) == len(REQUIRED_CANDIDATE_GROUPS),
            "candidate_coverage_gap",
            {"required_candidate_group_count": len(REQUIRED_CANDIDATE_GROUPS)},
        ),
        _requirement(
            "candidate_status_values_valid",
            all(row.get("candidate_status") in CANDIDATE_STATUS_VALUES for row in rows),
            "candidate_status_gap",
            {"status_values": list(CANDIDATE_STATUS_VALUES)},
        ),
        _requirement(
            "prior_research_doc_evidence_present",
            _research_text_covers_candidates(research_text),
            "research_doc_gap",
            {"required_candidate_groups": _required_candidate_group_ids()},
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


def _source_2430_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2430_STATUS
        and payload.get("promotion_evidence_review_ready") is True
        and payload.get("promotion_candidate_found") is False
        and payload.get("recommended_next_research_task") == EXPECTED_2430_NEXT_ROUTE
    )


def _candidate_registry_ready(registry: Mapping[str, Any]) -> bool:
    families = registry.get("candidate_families")
    safety = registry.get("safety_boundary")
    return (
        bool(registry.get("policy_id"))
        and isinstance(families, Sequence)
        and not isinstance(families, str)
        and len(families) > 0
        and isinstance(safety, Mapping)
        and safety.get("research_only") is True
        and safety.get("paper_shadow_allowed") is False
        and safety.get("production_allowed") is False
        and safety.get("broker_action") == "none"
    )


def _prior_candidate_evidence_ready(evidence: Mapping[str, Any]) -> bool:
    return (
        bool(evidence.get("current_best_candidate"))
        and bool(evidence.get("owner_decision"))
        and isinstance(evidence.get("component_value_candidates"), Sequence)
        and evidence.get("research_only_observation_approved") is False
        and evidence.get("paper_shadow_enabled") is False
        and evidence.get("production_enabled") is False
        and evidence.get("broker_action") == "none"
    )


def _component_value_evidence_ready(matrix: Mapping[str, Any]) -> bool:
    candidates = matrix.get("component_value_candidates")
    return isinstance(candidates, Sequence) and not isinstance(candidates, str) and bool(
        candidates
    )


def _component_value_ids(
    prior_evidence: Mapping[str, Any],
    component_matrix: Mapping[str, Any],
) -> set[str]:
    values: set[str] = set()
    for source in (
        prior_evidence.get("component_value_candidates"),
        component_matrix.get("component_value_candidates"),
    ):
        if isinstance(source, Sequence) and not isinstance(source, str):
            values.update(str(item) for item in source)
    entries = component_matrix.get("components")
    if isinstance(entries, Sequence) and not isinstance(entries, str):
        for entry in entries:
            if isinstance(entry, Mapping) and entry.get("component_value") is True:
                values.add(str(entry.get("candidate_id", "")))
    return {value for value in values if value}


def _promotion_candidate_ids(source_2430: Mapping[str, Any]) -> set[str]:
    matrix = source_2430.get("candidate_evidence_matrix")
    if not isinstance(matrix, Mapping):
        return set()
    candidates = matrix.get("candidates")
    if not isinstance(candidates, Sequence) or isinstance(candidates, str):
        return set()
    return {
        str(row.get("candidate_id"))
        for row in candidates
        if isinstance(row, Mapping) and row.get("paper_shadow_promotion_candidate") is True
    }


def _registry_candidate_ids(registry: Mapping[str, Any]) -> set[str]:
    families = registry.get("candidate_families")
    if not isinstance(families, Sequence) or isinstance(families, str):
        return set()
    return {
        str(family.get("strategy_id"))
        for family in families
        if isinstance(family, Mapping) and family.get("strategy_id")
    }


def _evidence_references(candidate_ids: Sequence[str], research_text: str) -> list[str]:
    return [candidate_id for candidate_id in candidate_ids if candidate_id in research_text]


def _metric_status_for_group(
    candidate_group_id: str,
    prior_evidence: Mapping[str, Any],
    evidence_refs: Sequence[str],
) -> dict[str, Any]:
    supporting = prior_evidence.get("candidate_owner_review_record")
    supporting_metrics = (
        supporting.get("supporting_metrics", {}) if isinstance(supporting, Mapping) else {}
    )
    failure_metrics = (
        supporting.get("failure_metrics", {}) if isinstance(supporting, Mapping) else {}
    )
    available_metrics = _available_metric_values(
        candidate_group_id,
        supporting_metrics if isinstance(supporting_metrics, Mapping) else {},
        failure_metrics if isinstance(failure_metrics, Mapping) else {},
    )
    rows: list[dict[str, Any]] = []
    for metric in RECOMMENDED_METRICS:
        if metric in available_metrics:
            coverage_status = "prior_metric_available"
            value = available_metrics[metric]
        elif evidence_refs:
            coverage_status = "prior_doc_reference_only"
            value = None
        else:
            coverage_status = "missing_from_prior_evidence"
            value = None
        rows.append(
            {
                "metric_id": metric,
                "coverage_status": coverage_status,
                "value": value,
                "computed_in_2431": False,
            }
        )
    if all(row["coverage_status"] == "prior_metric_available" for row in rows):
        coverage_status = "available"
    elif any(row["coverage_status"] == "prior_metric_available" for row in rows) or evidence_refs:
        coverage_status = "partial"
    else:
        coverage_status = "missing"
    return {"coverage_status": coverage_status, "metrics": rows}


def _available_metric_values(
    candidate_group_id: str,
    supporting_metrics: Mapping[str, Any],
    failure_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    if candidate_group_id != "equal_risk_growth_tilt_vol_target_variants":
        return {}
    return {
        "return_delta_vs_baseline": supporting_metrics.get("dynamic_vs_static_gap"),
        "max_drawdown_delta_vs_baseline": failure_metrics.get("drawdown_gap_vs_static"),
        "turnover_delta_vs_baseline": supporting_metrics.get("turnover"),
        "valid_until_hit_rate": (
            1.0 if supporting_metrics.get("valid_until_window_preserved") is True else None
        ),
        "regime_robustness_score": supporting_metrics.get("regime_slice_pass_rate"),
    }


def _metric_coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    status_counts = Counter(row.get("metric_coverage_status") for row in rows)
    return {
        "schema_version": CANDIDATE_METRIC_COVERAGE_SCHEMA_VERSION,
        "status": READY_STATUS,
        "candidate_metric_coverage_ready": bool(rows),
        "recommended_metrics": list(RECOMMENDED_METRICS),
        "candidate_metric_coverage": [
            {
                "candidate_group_id": row["candidate_group_id"],
                "coverage_status": row["metric_coverage_status"],
                "metrics": row["metric_coverage"],
            }
            for row in rows
        ],
        "metric_coverage_available_count": status_counts["available"],
        "metric_coverage_partial_count": status_counts["partial"],
        "metric_coverage_missing_count": status_counts["missing"],
        "computed_new_metrics": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_blockers(
    status: str,
    source_2430: Mapping[str, Any],
    prior_evidence: Mapping[str, Any],
    evidence_refs: Sequence[str],
) -> list[str]:
    blockers: list[str] = []
    if status != "promotion_candidate":
        blockers.extend(
            [
                "not_paper_shadow_promotion_candidate",
                "paper_shadow_not_approved",
                "production_not_approved",
                "broker_not_approved",
            ]
        )
    if source_2430.get("promotion_candidate_found") is not True:
        blockers.append("trading_2430_no_promotion_candidate")
    if prior_evidence.get("research_only_observation_approved") is not True:
        blockers.append("prior_owner_observation_not_approved")
    if not evidence_refs:
        blockers.append("prior_doc_reference_missing")
    blockers.append("requires_gauntlet_or_pit_before_paper_shadow_review")
    return blockers


def _status_rationale(
    status: str,
    group: Mapping[str, Any],
    prior_evidence: Mapping[str, Any],
) -> str:
    if status == "promotion_candidate":
        return "Existing prior evidence marks this group as a promotion candidate."
    if status == "component_value":
        return (
            f"Prior evidence supports component-level value for "
            f"{group['candidate_group_id']}; this is not paper-shadow approval."
        )
    if status == "needs_pit":
        current_best = prior_evidence.get("current_best_candidate")
        return (
            f"{group['candidate_group_id']} needs batch gauntlet or PIT replay; "
            f"current best candidate is {current_best} but owner approval is absent."
        )
    return "Existing evidence rejects this group for current promotion purposes."


def _research_text_covers_candidates(research_text: str) -> bool:
    return all(
        any(candidate_id in research_text for candidate_id in group["source_candidate_ids"])
        for group in REQUIRED_CANDIDATE_GROUPS
    )


def _required_candidate_group_ids() -> list[str]:
    return [str(group["candidate_group_id"]) for group in REQUIRED_CANDIDATE_GROUPS]


def _no_effect_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": not gaps,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "fresh_market_data_read": False,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "candidate_gauntlet_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
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


def _contains_all(text: str, references: Sequence[str]) -> bool:
    return all(reference in text for reference in references)


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
        if isinstance(report, Mapping)
    }
    return all(report_id in present for report_id in report_ids)
