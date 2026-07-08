from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_candidate_promotion_evidence_review.v1"
CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_engine_candidate_evidence_matrix.v1"
)
CANDIDATE_DECISION_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_candidate_decision_summary.v1"
)
NO_PROMOTION_RATIONALE_SCHEMA_VERSION = (
    "growth_tilt_engine_no_promotion_rationale.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_engine_candidate_promotion_no_effect_boundary.v1"
)

NO_PROMOTION_STATUS = (
    "GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_"
    "NO_PROMOTION_CANDIDATE"
)
PROMOTION_CANDIDATE_FOUND_STATUS = (
    "GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_"
    "PROMOTION_CANDIDATE_FOUND"
)
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_BLOCKED_BY_"
    "EVIDENCE_GAPS"
)
NEXT_ROUTE_NO_CANDIDATE = (
    "TRADING-2431_Growth_Tilt_Existing_Candidate_Evidence_Matrix"
)
NEXT_ROUTE_CANDIDATE_FOUND = (
    "TRADING-2431_Growth_Tilt_Candidate_Specific_Paper_Shadow_Gate"
)
NEXT_ROUTE_BLOCKED = (
    "TRADING-2431_Growth_Tilt_Candidate_Promotion_Evidence_Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2430_Growth_Tilt_Engine_Candidate_Promotion_Evidence_Review"
EXPECTED_2426_STATUS = "GROWTH_TILT_ENGINE_PAPER_SHADOW_SCHEDULE_DRY_RUN_READY"
EXPECTED_2427_STATUS = "GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN_READY"
EXPECTED_2428_STATUS = "GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_READY"
EXPECTED_2429_STATUS = "GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY_READY"
EXPECTED_2429_NEXT_ROUTE = (
    "TRADING-2430_Growth_Tilt_Engine_Candidate_Promotion_Evidence_Review"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
REPORT_TYPE = "growth_tilt_engine_candidate_promotion_evidence_review"

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_paper_shadow_schedule_dry_run",
    "growth_tilt_engine_manual_review_packet_dry_run",
    "growth_tilt_engine_observe_only_signal_artifact_boundary",
    "growth_tilt_engine_forward_outcome_binding_boundary",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-candidate-promotion-evidence-review",
    "outputs/research_strategies/"
    "growth_tilt_engine_candidate_promotion_evidence_review/"
    "promotion_evidence_review_result.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_candidate_promotion_evidence_review/"
    "candidate_evidence_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_candidate_promotion_evidence_review/"
    "candidate_decision_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_candidate_promotion_evidence_review/"
    "no_promotion_rationale.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_candidate_promotion_evidence_review/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_engine_candidate_promotion_evidence_review.md",
    "docs/research/growth_tilt_engine_candidate_evidence_matrix.md",
    "docs/research/growth_tilt_engine_candidate_decision_summary.md",
    "docs/research/growth_tilt_engine_no_promotion_rationale.md",
    "docs/research/growth_tilt_engine_candidate_promotion_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2431_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-candidate-promotion-evidence-review",
    NO_PROMOTION_STATUS,
    NEXT_ROUTE_NO_CANDIDATE,
)


def build_growth_tilt_engine_candidate_promotion_evidence_review(
    schedule_dry_run_result_2426: Mapping[str, Any],
    manual_review_packet_dry_run_result_2427: Mapping[str, Any],
    observe_only_boundary_result_2428: Mapping[str, Any],
    forward_outcome_binding_boundary_result_2429: Mapping[str, Any],
    candidate_registry: Mapping[str, Any],
    prior_candidate_evidence: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    candidate_rows = _candidate_rows(candidate_registry, prior_candidate_evidence)
    promotion_candidates = [
        row for row in candidate_rows if row["paper_shadow_promotion_candidate"] is True
    ]
    requirements = _promotion_review_requirements(
        schedule_dry_run_result_2426,
        manual_review_packet_dry_run_result_2427,
        observe_only_boundary_result_2428,
        forward_outcome_binding_boundary_result_2429,
        candidate_registry,
        prior_candidate_evidence,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_doc_texts=research_doc_texts or {},
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in requirements
        if requirement["status"] != "PASS"
    ]
    review_ready = not gaps
    if not review_ready:
        status = BLOCKED_STATUS
        next_route = NEXT_ROUTE_BLOCKED
    elif promotion_candidates:
        status = PROMOTION_CANDIDATE_FOUND_STATUS
        next_route = NEXT_ROUTE_CANDIDATE_FOUND
    else:
        status = NO_PROMOTION_STATUS
        next_route = NEXT_ROUTE_NO_CANDIDATE

    candidate_decision_summary = {
        "schema_version": CANDIDATE_DECISION_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "promotion_evidence_review_ready": review_ready,
        "promotion_candidate_found": bool(promotion_candidates),
        "promotion_candidate_count": len(promotion_candidates),
        "candidate_count": len(candidate_rows),
        "candidate_decisions": candidate_rows,
        "next_route_if_no_candidate": NEXT_ROUTE_NO_CANDIDATE,
        "next_route_if_candidate_found": NEXT_ROUTE_CANDIDATE_FOUND,
        "production_effect": "none",
        "broker_action": "none",
    }
    no_promotion_rationale = _no_promotion_rationale(
        status,
        candidate_registry,
        prior_candidate_evidence,
        promotion_candidates=promotion_candidates,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2430",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "source_tasks": ["TRADING-2426", "TRADING-2427", "TRADING-2428", "TRADING-2429"],
        "schedule_dry_run_ready": _schedule_ready(schedule_dry_run_result_2426),
        "manual_review_packet_dry_run_ready": _manual_packet_ready(
            manual_review_packet_dry_run_result_2427
        ),
        "observe_only_signal_artifact_boundary_ready": _observe_only_ready(
            observe_only_boundary_result_2428
        ),
        "forward_outcome_binding_boundary_ready": _forward_outcome_ready(
            forward_outcome_binding_boundary_result_2429
        ),
        "candidate_registry_ready": _candidate_registry_ready(candidate_registry),
        "prior_candidate_evidence_ready": _prior_candidate_evidence_ready(
            prior_candidate_evidence
        ),
        "promotion_evidence_review_started": True,
        "promotion_evidence_review_completed": review_ready,
        "promotion_evidence_review_ready": review_ready,
        "promotion_candidate_found": bool(promotion_candidates),
        "promotion_candidate_count": len(promotion_candidates),
        "candidate_count": len(candidate_rows),
        "candidate_evidence_matrix_ready": bool(candidate_rows),
        "candidate_decision_summary_ready": review_ready,
        "no_promotion_rationale_ready": (
            review_ready and not bool(promotion_candidates)
        ),
        "engineering_readiness_is_alpha_evidence": False,
        "paper_shadow_promotion_allowed_by_registry": _registry_allows_paper_shadow(
            candidate_registry
        ),
        "prior_owner_approved_paper_shadow": _prior_approved_paper_shadow(
            prior_candidate_evidence
        ),
        "prior_owner_approved_observation": _prior_approved_observation(
            prior_candidate_evidence
        ),
        "promotion_evidence_review_gap_count": len(gaps),
        "promotion_evidence_review_gap_ids": [
            gap["requirement_id"] for gap in gaps
        ],
        "missing_promotion_review_evidence_count": _gap_count(
            gaps,
            "missing_promotion_review_evidence",
        ),
        "safety_boundary_gap_count": _gap_count(gaps, "promotion_safety_boundary"),
        "candidate_evidence_gap_count": _gap_count(gaps, "candidate_evidence_gap"),
        "precondition_gap_count": _gap_count(gaps, "promotion_precondition_gap"),
        "requirements": requirements,
        "gaps": gaps,
        "candidate_evidence_matrix": {
            "schema_version": CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION,
            "status": status,
            "candidate_evidence_matrix_ready": bool(candidate_rows),
            "candidate_count": len(candidate_rows),
            "candidates": candidate_rows,
            "production_effect": "none",
            "broker_action": "none",
        },
        "candidate_decision_summary": candidate_decision_summary,
        "no_promotion_rationale": no_promotion_rationale,
        "no_effect_boundary": no_effect_boundary,
        "observe_only": True,
        "manual_review_required": True,
        "manual_review_only": True,
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
        "next_route_if_no_candidate": NEXT_ROUTE_NO_CANDIDATE,
        "next_route_if_candidate_found": NEXT_ROUTE_CANDIDATE_FOUND,
        "recommended_next_research_task_reason": (
            "No paper-shadow promotion candidate is present; continue with "
            "existing candidate evidence matrix."
            if status == NO_PROMOTION_STATUS
            else "At least one candidate needs candidate-specific paper-shadow gate."
            if status == PROMOTION_CANDIDATE_FOUND_STATUS
            else "Promotion evidence review has unresolved gaps."
        ),
    }


def _promotion_review_requirements(
    schedule_result: Mapping[str, Any],
    manual_result: Mapping[str, Any],
    observe_result: Mapping[str, Any],
    outcome_result: Mapping[str, Any],
    candidate_registry: Mapping[str, Any],
    prior_candidate_evidence: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str],
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "prior_2426_schedule_dry_run_ready",
            _schedule_ready(schedule_result),
            "promotion_precondition_gap",
            {"status": schedule_result.get("status")},
        ),
        _requirement(
            "prior_2427_manual_review_packet_ready",
            _manual_packet_ready(manual_result),
            "promotion_precondition_gap",
            {"status": manual_result.get("status")},
        ),
        _requirement(
            "prior_2428_observe_only_boundary_ready",
            _observe_only_ready(observe_result),
            "promotion_precondition_gap",
            {"status": observe_result.get("status")},
        ),
        _requirement(
            "prior_2429_forward_outcome_boundary_ready",
            _forward_outcome_ready(outcome_result),
            "promotion_precondition_gap",
            {
                "status": outcome_result.get("status"),
                "next_route": outcome_result.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "candidate_registry_ready",
            _candidate_registry_ready(candidate_registry),
            "candidate_evidence_gap",
            {"policy_id": candidate_registry.get("policy_id")},
        ),
        _requirement(
            "candidate_registry_safety_boundary_reviewed",
            _registry_safety_boundary_reviewed(candidate_registry),
            "promotion_safety_boundary",
            _safety_boundary(candidate_registry),
        ),
        _requirement(
            "prior_candidate_evidence_ready",
            _prior_candidate_evidence_ready(prior_candidate_evidence),
            "candidate_evidence_gap",
            {
                "current_best_candidate": prior_candidate_evidence.get(
                    "current_best_candidate"
                ),
                "owner_decision": prior_candidate_evidence.get("owner_decision"),
            },
        ),
        _requirement(
            "prior_candidate_paper_shadow_evidence_reviewed",
            "paper_shadow_approved" in prior_candidate_evidence
            or "paper_shadow_allowed" in prior_candidate_evidence,
            "promotion_safety_boundary",
            {"paper_shadow_approved": prior_candidate_evidence.get("paper_shadow_approved")},
        ),
        _requirement(
            "report_registry_coverage",
            _report_registry_has(report_registry, REQUIRED_REPORT_IDS),
            "missing_promotion_review_evidence",
            {"required_report_ids": list(REQUIRED_REPORT_IDS)},
        ),
        _requirement(
            "artifact_catalog_coverage",
            _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES),
            "missing_promotion_review_evidence",
            {"required_references": list(REQUIRED_CATALOG_REFERENCES)},
        ),
        _requirement(
            "system_flow_coverage",
            _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES),
            "missing_promotion_review_evidence",
            {"required_references": list(REQUIRED_SYSTEM_FLOW_REFERENCES)},
        ),
        _requirement(
            "research_doc_coverage",
            _research_docs_cover_route(research_doc_texts),
            "missing_promotion_review_evidence",
            {"required_route": EXPECTED_2429_NEXT_ROUTE},
        ),
    ]


def _candidate_rows(
    candidate_registry: Mapping[str, Any],
    prior_candidate_evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    families = candidate_registry.get("candidate_families")
    if not isinstance(families, Sequence):
        families = []
    current_best = str(prior_candidate_evidence.get("current_best_candidate", ""))
    rows: list[dict[str, Any]] = []
    for family in families:
        if not isinstance(family, Mapping):
            continue
        strategy_id = str(family.get("strategy_id", ""))
        is_current_best = strategy_id == current_best
        prior_decision = (
            prior_candidate_evidence.get("current_best_candidate_preview_decision")
            if is_current_best
            else "NOT_CURRENT_BEST"
        )
        owner_decision = (
            prior_candidate_evidence.get("owner_decision")
            if is_current_best
            else "NO_OWNER_APPROVAL_FOR_THIS_CANDIDATE"
        )
        promotion_candidate = (
            is_current_best
            and family.get("paper_shadow_allowed") is True
            and _registry_allows_paper_shadow(candidate_registry)
            and _prior_approved_paper_shadow(prior_candidate_evidence)
            and _prior_approved_observation(prior_candidate_evidence)
            and prior_candidate_evidence.get(
                "current_best_candidate_previous_decision"
            )
            == "ACCEPT_FOR_SHADOW_RESEARCH"
        )
        rows.append(
            {
                "candidate_id": strategy_id,
                "candidate_family": family.get("candidate_family"),
                "is_current_best_candidate": is_current_best,
                "prior_decision": prior_decision,
                "owner_decision": owner_decision,
                "paper_shadow_allowed_by_candidate": family.get(
                    "paper_shadow_allowed"
                )
                is True,
                "production_allowed_by_candidate": family.get("production_allowed")
                is True,
                "broker_action": family.get("broker_action", "none"),
                "paper_shadow_promotion_candidate": promotion_candidate,
                "promotion_blockers": _candidate_blockers(
                    family,
                    prior_candidate_evidence,
                    is_current_best=is_current_best,
                ),
                "production_effect": "none",
            }
        )
    if not rows and current_best:
        rows.append(
            {
                "candidate_id": current_best,
                "candidate_family": "unknown_from_prior_evidence",
                "is_current_best_candidate": True,
                "prior_decision": prior_candidate_evidence.get(
                    "current_best_candidate_preview_decision"
                ),
                "owner_decision": prior_candidate_evidence.get("owner_decision"),
                "paper_shadow_allowed_by_candidate": False,
                "production_allowed_by_candidate": False,
                "broker_action": "none",
                "paper_shadow_promotion_candidate": False,
                "promotion_blockers": ["candidate_missing_from_registry"],
                "production_effect": "none",
            }
        )
    return rows


def _candidate_blockers(
    family: Mapping[str, Any],
    prior_candidate_evidence: Mapping[str, Any],
    *,
    is_current_best: bool,
) -> list[str]:
    blockers: list[str] = []
    if not is_current_best:
        blockers.append("not_current_best_candidate")
    if family.get("paper_shadow_allowed") is not True:
        blockers.append("candidate_registry_paper_shadow_allowed_false")
    if _prior_approved_paper_shadow(prior_candidate_evidence) is not True:
        blockers.append("prior_owner_paper_shadow_not_approved")
    if _prior_approved_observation(prior_candidate_evidence) is not True:
        blockers.append("prior_owner_observation_not_approved")
    if prior_candidate_evidence.get("current_best_candidate_previous_decision") != (
        "ACCEPT_FOR_SHADOW_RESEARCH"
    ):
        blockers.append("prior_decision_not_shadow_research_accept")
    blockers.append("engineering_readiness_is_not_alpha_evidence")
    return blockers


def _no_promotion_rationale(
    status: str,
    candidate_registry: Mapping[str, Any],
    prior_candidate_evidence: Mapping[str, Any],
    *,
    promotion_candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reasons = [
        "candidate_registry_paper_shadow_allowed_false",
        "prior_owner_paper_shadow_not_approved",
        "prior_owner_observation_not_approved",
        "prior_decision_not_shadow_research_accept",
        "engineering_readiness_is_not_alpha_evidence",
    ]
    return {
        "schema_version": NO_PROMOTION_RATIONALE_SCHEMA_VERSION,
        "status": status,
        "no_promotion_rationale_ready": status == NO_PROMOTION_STATUS,
        "promotion_candidate_count": len(promotion_candidates),
        "promotion_candidate_found": bool(promotion_candidates),
        "current_best_candidate": prior_candidate_evidence.get(
            "current_best_candidate"
        ),
        "current_best_candidate_previous_decision": prior_candidate_evidence.get(
            "current_best_candidate_previous_decision"
        ),
        "owner_decision": prior_candidate_evidence.get("owner_decision"),
        "candidate_registry_paper_shadow_allowed": _registry_allows_paper_shadow(
            candidate_registry
        ),
        "rationale": [] if promotion_candidates else reasons,
        "next_route": (
            NEXT_ROUTE_CANDIDATE_FOUND
            if promotion_candidates
            else NEXT_ROUTE_NO_CANDIDATE
        ),
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
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "fresh_market_data_read": False,
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
        "promotion_evidence_review_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _schedule_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2426_STATUS
        and payload.get("paper_shadow_schedule_dry_run_ready") is True
    )


def _manual_packet_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2427_STATUS
        and payload.get("manual_review_packet_dry_run_ready") is True
    )


def _observe_only_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2428_STATUS
        and payload.get("observe_only_signal_artifact_boundary_ready") is True
    )


def _forward_outcome_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2429_STATUS
        and payload.get("forward_outcome_binding_boundary_ready") is True
        and payload.get("recommended_next_research_task") == EXPECTED_2429_NEXT_ROUTE
    )


def _candidate_registry_ready(registry: Mapping[str, Any]) -> bool:
    families = registry.get("candidate_families")
    return (
        bool(registry.get("policy_id"))
        and _safety_boundary(registry).get("research_only") is True
        and isinstance(families, Sequence)
        and not isinstance(families, str)
        and len(families) > 0
    )


def _prior_candidate_evidence_ready(evidence: Mapping[str, Any]) -> bool:
    return (
        bool(evidence.get("current_best_candidate"))
        and bool(evidence.get("owner_decision"))
        and evidence.get("paper_shadow_enabled") is False
        and evidence.get("production_enabled") is False
        and evidence.get("broker_action") == "none"
    )


def _safety_boundary(registry: Mapping[str, Any]) -> Mapping[str, Any]:
    value = registry.get("safety_boundary")
    return value if isinstance(value, Mapping) else {}


def _registry_allows_paper_shadow(registry: Mapping[str, Any]) -> bool:
    return _safety_boundary(registry).get("paper_shadow_allowed") is True


def _registry_safety_boundary_reviewed(registry: Mapping[str, Any]) -> bool:
    safety = _safety_boundary(registry)
    return (
        isinstance(safety.get("paper_shadow_allowed"), bool)
        and safety.get("production_allowed") is False
        and safety.get("broker_action") == "none"
    )


def _prior_approved_paper_shadow(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence.get("paper_shadow_approved") is True
        or evidence.get("paper_shadow_allowed") is True
    )


def _prior_approved_observation(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence.get("research_only_observation_approved") is True
        or evidence.get("observation_approved") is True
    )


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


def _gap_count(gaps: Sequence[Mapping[str, Any]], classification: str) -> int:
    return sum(1 for gap in gaps if gap.get("classification") == classification)


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


def _research_docs_cover_route(research_doc_texts: Mapping[str, str]) -> bool:
    joined = "\n".join(str(text) for text in research_doc_texts.values())
    return EXPECTED_2429_STATUS in joined and EXPECTED_2429_NEXT_ROUTE in joined
