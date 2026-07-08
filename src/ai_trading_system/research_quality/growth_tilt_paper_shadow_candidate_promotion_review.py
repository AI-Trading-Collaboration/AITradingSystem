from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_paper_shadow_candidate_promotion_review.v1"
EVIDENCE_SUMMARY_SCHEMA_VERSION = "growth_tilt_paper_shadow_candidate_evidence_summary.v1"
CANDIDATE_DECISION_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_paper_shadow_candidate_decision_matrix.v1"
)
BLOCKED_PROMOTION_ROUTE_SCHEMA_VERSION = (
    "growth_tilt_paper_shadow_candidate_blocked_route.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = "growth_tilt_paper_shadow_candidate_no_effect.v1"

CANDIDATE_FOUND_STATUS = (
    "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_CANDIDATE_FOUND"
)
NO_CANDIDATE_STATUS = (
    "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_NO_CANDIDATE"
)
BLOCKED_FORWARD_AGING_STATUS = (
    "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE"
)
BLOCKED_DATA_QUALITY_STATUS = (
    "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_DATA_QUALITY_GATE"
)
BLOCKED_EVIDENCE_STATUS = (
    "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2431_STATUS = "GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY"
EXPECTED_2432_STATUS = "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
EXPECTED_2434_STATUS = "GROWTH_TILT_DEFENSIVE_LIMITED_ADJUSTMENT_COMPONENT_VALIDATION_READY"
EXPECTED_2437_STATUS = "GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY"
EXPECTED_2438_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_READY"
EXPECTED_2439_STATUS = "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_READY"
EXPECTED_2439_NEXT_ROUTE = (
    "TRADING-2440_Growth_Tilt_Paper_Shadow_Candidate_Promotion_Review"
)
NEXT_ROUTE_IF_CANDIDATE_FOUND = (
    "TRADING-2441_Growth_Tilt_Candidate_Specific_Paper_Shadow_Gate"
)
NEXT_ROUTE_IF_NO_CANDIDATE = (
    "TRADING-2441_Growth_Tilt_Strategy_Research_Backlog_Reprioritization"
)
BLOCKED_ROUTE = "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
REPORT_TYPE = "growth_tilt_paper_shadow_candidate_promotion_review"
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_existing_candidate_evidence_matrix",
    "growth_tilt_candidate_gauntlet_harness",
    "growth_tilt_defensive_limited_adjustment_component_validation",
    "growth_tilt_regime_slice_attribution_review",
    "growth_tilt_top3_candidate_pit_replay",
    "growth_tilt_forward_aging_candidate_pack",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-paper-shadow-candidate-promotion-review",
    "outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/"
    "promotion_review_result.json",
    "outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/"
    "evidence_summary.json",
    "outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/"
    "candidate_decision_matrix.json",
    "outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/"
    "blocked_promotion_route.json",
    "outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_paper_shadow_candidate_promotion_review.md",
    "docs/research/growth_tilt_paper_shadow_candidate_evidence_summary.md",
    "docs/research/growth_tilt_paper_shadow_candidate_decision_matrix.md",
    "docs/research/growth_tilt_paper_shadow_candidate_blocked_route.md",
    "docs/research/growth_tilt_paper_shadow_candidate_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2440_blocked_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-paper-shadow-candidate-promotion-review",
    BLOCKED_FORWARD_AGING_STATUS,
    BLOCKED_ROUTE,
)
FORWARD_AGING_REQUIREMENT_IDS: tuple[str, ...] = (
    "source_2439_forward_aging_candidate_pack_ready",
    "source_2438_pit_replay_ready_for_promotion_review",
)


def build_growth_tilt_paper_shadow_candidate_promotion_review(
    source_2431_existing_candidate_evidence: Mapping[str, Any],
    source_2432_candidate_gauntlet: Mapping[str, Any],
    source_2434_component_validation: Mapping[str, Any],
    source_2437_regime_review: Mapping[str, Any],
    source_2438_pit_replay: Mapping[str, Any],
    source_2439_forward_pack: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    review_rows = _candidate_review_rows(source_2439_forward_pack)
    selected_candidates = [
        row for row in review_rows if row.get("paper_shadow_candidate") is True
    ]
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _requirements(
        source_2431_existing_candidate_evidence,
        source_2432_candidate_gauntlet,
        source_2434_component_validation,
        source_2437_regime_review,
        source_2438_pit_replay,
        source_2439_forward_pack,
        data_quality_summary,
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
    status = _status_from_gaps(gaps, selected_candidates)
    if status not in {CANDIDATE_FOUND_STATUS, NO_CANDIDATE_STATUS}:
        review_rows = []
        selected_candidates = []
    evidence_summary = _evidence_summary(
        status,
        source_2431_existing_candidate_evidence,
        source_2432_candidate_gauntlet,
        source_2434_component_validation,
        source_2437_regime_review,
        source_2438_pit_replay,
        source_2439_forward_pack,
        gaps,
    )
    decision_matrix = _candidate_decision_matrix(status, review_rows, gaps)
    blocked_route = _blocked_promotion_route(status, gaps)
    boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2440",
        "status": status,
        "readiness_status": status,
        "source_tasks": [
            "TRADING-2431",
            "TRADING-2432",
            "TRADING-2434",
            "TRADING-2437",
            "TRADING-2438",
            "TRADING-2439",
        ],
        "source_2431_ready": _source_2431_ready(source_2431_existing_candidate_evidence),
        "source_2432_ready": _source_2432_ready(source_2432_candidate_gauntlet),
        "source_2434_ready": _source_2434_ready(source_2434_component_validation),
        "source_2437_ready": _source_2437_ready(source_2437_regime_review),
        "source_2438_ready": _source_2438_ready(source_2438_pit_replay),
        "source_2439_forward_aging_ready": _source_2439_ready(source_2439_forward_pack),
        "forward_aging_source_status": source_2439_forward_pack.get("status"),
        "pit_replay_source_status": source_2438_pit_replay.get("status"),
        "promotion_review_ready": status
        in {CANDIDATE_FOUND_STATUS, NO_CANDIDATE_STATUS},
        "evidence_summary_ready": evidence_summary["evidence_summary_ready"],
        "candidate_decision_matrix_ready": decision_matrix[
            "candidate_decision_matrix_ready"
        ],
        "blocked_promotion_route_ready": blocked_route[
            "blocked_promotion_route_ready"
        ],
        "no_effect_boundary_ready": boundary["no_effect_boundary_ready"],
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "forward_aging_candidate_count": int(
            source_2439_forward_pack.get("forward_aging_candidate_count", 0) or 0
        ),
        "review_candidate_count": len(review_rows),
        "paper_shadow_candidate_found": len(selected_candidates) > 0,
        "paper_shadow_candidate_count": len(selected_candidates),
        "selected_candidates": selected_candidates,
        "candidate_decision_rows": review_rows,
        "evidence_summary": evidence_summary,
        "candidate_decision_matrix": decision_matrix,
        "blocked_promotion_route": blocked_route,
        "no_effect_boundary": boundary,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "market_data_experiment_run": False,
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "candidate_tracking_started": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
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
        "recommended_next_research_task": _next_route(status),
        "recommended_next_research_task_reason": _next_route_reason(status),
    }


def _requirements(
    source_2431: Mapping[str, Any],
    source_2432: Mapping[str, Any],
    source_2434: Mapping[str, Any],
    source_2437: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    source_2439: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2431_existing_candidate_evidence_ready",
            _source_2431_ready(source_2431),
            "prior_evidence_gap",
            {"status": source_2431.get("status")},
        ),
        _requirement(
            "source_2432_candidate_gauntlet_ready",
            _source_2432_ready(source_2432),
            "prior_evidence_gap",
            {"status": source_2432.get("status")},
        ),
        _requirement(
            "source_2434_component_validation_ready",
            _source_2434_ready(source_2434),
            "prior_evidence_gap",
            {"status": source_2434.get("status")},
        ),
        _requirement(
            "source_2437_regime_slice_attribution_ready",
            _source_2437_ready(source_2437),
            "prior_evidence_gap",
            {"status": source_2437.get("status")},
        ),
        _requirement(
            "source_2438_pit_replay_ready_for_promotion_review",
            _source_2438_ready(source_2438),
            "forward_aging_gate_gap",
            {
                "status": source_2438.get("status"),
                "pit_replay_pass_count": source_2438.get("pit_replay_pass_count"),
            },
        ),
        _requirement(
            "source_2439_forward_aging_candidate_pack_ready",
            _source_2439_ready(source_2439),
            "forward_aging_gate_gap",
            {
                "status": source_2439.get("status"),
                "forward_aging_candidate_count": source_2439.get(
                    "forward_aging_candidate_count"
                ),
                "next_route": source_2439.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_gap",
            {
                "data_quality_status": data_quality_summary.get("data_quality_status"),
                "report_path": data_quality_summary.get("data_quality_report_path"),
            },
        ),
        _requirement(
            "prior_research_doc_coverage",
            "promotion" in research_text or "paper-shadow" in research_text,
            "research_doc_gap",
            {"required_references": ["promotion", "paper-shadow"]},
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
        and payload.get("candidate_status_summary_ready") is True
    )


def _source_2432_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2432_STATUS
        and payload.get("harness_ready") is True
        and payload.get("candidate_gauntlet_run") is False
    )


def _source_2434_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2434_STATUS
        and payload.get("component_validation_ready") is True
        and payload.get("component_value_found") is True
    )


def _source_2437_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2437_STATUS
        and payload.get("regime_slice_attribution_review_ready") is True
        and payload.get("regime_attribution_run") is False
    )


def _source_2438_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438_STATUS
        and int(payload.get("pit_replay_pass_count", 0) or 0) > 0
        and payload.get("pit_replay_executed") is True
    )


def _source_2439_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2439_STATUS
        and payload.get("forward_aging_candidate_pack_ready") is True
        and payload.get("recommended_next_research_task") == EXPECTED_2439_NEXT_ROUTE
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _candidate_review_rows(source_2439: Mapping[str, Any]) -> list[dict[str, Any]]:
    if not _source_2439_ready(source_2439):
        return []
    pack = _mapping(source_2439.get("forward_aging_candidate_pack"))
    candidates = _sequence(pack.get("candidates"))
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        paper_shadow_candidate = candidate.get("paper_shadow_candidate") is True
        rows.append(
            {
                "candidate_id": candidate.get("candidate_id"),
                "status": "selected"
                if paper_shadow_candidate
                else "needs_more_forward_evidence",
                "primary_value": candidate.get("primary_value"),
                "key_risk": candidate.get(
                    "key_risk",
                    "forward_evidence_not_matured",
                ),
                "paper_shadow_candidate": paper_shadow_candidate,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return rows


def _status_from_gaps(
    gaps: Sequence[Mapping[str, Any]],
    selected_candidates: Sequence[Mapping[str, Any]],
) -> str:
    if gaps:
        gap_ids = {str(gap.get("requirement_id")) for gap in gaps}
        if "data_quality_gate_passed" in gap_ids:
            return BLOCKED_DATA_QUALITY_STATUS
        if gap_ids.intersection(FORWARD_AGING_REQUIREMENT_IDS):
            return BLOCKED_FORWARD_AGING_STATUS
        return BLOCKED_EVIDENCE_STATUS
    if selected_candidates:
        return CANDIDATE_FOUND_STATUS
    return NO_CANDIDATE_STATUS


def _evidence_summary(
    status: str,
    source_2431: Mapping[str, Any],
    source_2432: Mapping[str, Any],
    source_2434: Mapping[str, Any],
    source_2437: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    source_2439: Mapping[str, Any],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": EVIDENCE_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "evidence_summary_ready": True,
        "source_statuses": {
            "TRADING-2431": source_2431.get("status"),
            "TRADING-2432": source_2432.get("status"),
            "TRADING-2434": source_2434.get("status"),
            "TRADING-2437": source_2437.get("status"),
            "TRADING-2438": source_2438.get("status"),
            "TRADING-2439": source_2439.get("status"),
        },
        "forward_aging_candidate_count": source_2439.get(
            "forward_aging_candidate_count",
            0,
        ),
        "pit_replay_pass_count": source_2438.get("pit_replay_pass_count", 0),
        "paper_shadow_candidate_evidence_complete": status
        in {CANDIDATE_FOUND_STATUS, NO_CANDIDATE_STATUS},
        "evidence_gap_count": len(gaps),
        "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_decision_matrix(
    status: str,
    rows: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_DECISION_MATRIX_SCHEMA_VERSION,
        "status": status,
        "candidate_decision_matrix_ready": True,
        "paper_shadow_candidate_found": any(
            row.get("paper_shadow_candidate") is True for row in rows
        ),
        "paper_shadow_candidate_count": sum(
            1 for row in rows if row.get("paper_shadow_candidate") is True
        ),
        "selected_candidates": [
            dict(row) for row in rows if row.get("paper_shadow_candidate") is True
        ],
        "rows": list(rows),
        "blocked_reason": None
        if status in {CANDIDATE_FOUND_STATUS, NO_CANDIDATE_STATUS}
        else "forward_aging_gate_not_ready",
        "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
        "production_effect": "none",
        "broker_action": "none",
    }


def _blocked_promotion_route(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    blocked = status not in {CANDIDATE_FOUND_STATUS, NO_CANDIDATE_STATUS}
    return {
        "schema_version": BLOCKED_PROMOTION_ROUTE_SCHEMA_VERSION,
        "status": status,
        "blocked_promotion_route_ready": True,
        "promotion_review_blocked": blocked,
        "blocking_gap_count": len(gaps),
        "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
        "next_route": _next_route(status),
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
        "no_effect_boundary_ready": True,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "evidence_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _next_route(status: str) -> str:
    if status == CANDIDATE_FOUND_STATUS:
        return NEXT_ROUTE_IF_CANDIDATE_FOUND
    if status == NO_CANDIDATE_STATUS:
        return NEXT_ROUTE_IF_NO_CANDIDATE
    return BLOCKED_ROUTE


def _next_route_reason(status: str) -> str:
    if status == CANDIDATE_FOUND_STATUS:
        return "Paper-shadow candidate evidence is present for owner review."
    if status == NO_CANDIDATE_STATUS:
        return "No paper-shadow candidate passed the evidence review."
    return (
        "Forward aging candidate pack is not ready; remediate PIT replay and "
        "forward aging gates before promotion review."
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
