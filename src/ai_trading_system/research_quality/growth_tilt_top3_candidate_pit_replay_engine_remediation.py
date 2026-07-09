from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_top3_candidate_pit_replay_engine_remediation.v1"
REMEDIATION_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_engine_remediation_evidence.v1"
)
BEFORE_AFTER_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_engine_remediation_before_after.v1"
)
REMAINING_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_engine_remediation_remaining_blockers.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_engine_remediation_no_effect.v1"
)

READY_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_READY"
BLOCKED_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED"

EXPECTED_2440_STATUS = (
    "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE"
)
EXPECTED_2440_ROUTE = (
    "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
)
EXPECTED_2439_BLOCKED_STATUS = (
    "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE"
)
EXPECTED_2438_BLOCKED_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
)
EXPECTED_2438_READY_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_READY"
EXPECTED_2438_READY_ROUTE = "TRADING-2439_Growth_Tilt_Forward_Aging_Candidate_Pack"

NEXT_ROUTE_READY = "TRADING-2438B_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck"
NEXT_ROUTE_BLOCKED = "TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure"
REPORT_TYPE = "growth_tilt_top3_candidate_pit_replay_engine_remediation"

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_top3_candidate_pit_replay",
    "growth_tilt_forward_aging_candidate_pack",
    "growth_tilt_paper_shadow_candidate_promotion_review",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-top3-candidate-pit-replay-engine-remediation",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/"
    "remediation_result.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/"
    "remediation_evidence.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/"
    "before_after_comparison.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/"
    "remaining_blocker_summary.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation.md",
    "docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_evidence.md",
    "docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_before_after.md",
    "docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_remaining_blockers.md",
    "docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438B_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-pit-replay-engine-remediation",
    READY_STATUS,
    BLOCKED_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_BLOCKED,
)

REMEDIATION_REQUIREMENT_IDS: tuple[str, ...] = (
    "candidate_selection_resolves",
    "top3_candidate_ids_present",
    "pit_replay_artifacts_present",
    "candidate_pit_replay_engine_available",
    "candidate_replay_input_specs_ready",
    "source_traceability_complete",
    "as_of_boundary_explicit",
    "valid_until_boundary_explicit",
    "outcome_linkage_complete",
    "pit_replay_evidence_complete",
    "forward_aging_handoff_ready",
)


def build_growth_tilt_top3_candidate_pit_replay_engine_remediation(
    source_2440_promotion_review: Mapping[str, Any],
    source_2439_forward_pack: Mapping[str, Any],
    source_2438_pit_replay: Mapping[str, Any],
    pit_replay_evidence: Mapping[str, Any],
    pit_replay_blocker_summary: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    selected_candidates = _selected_candidates(source_2438_pit_replay)
    selected_ids = [
        str(candidate.get("candidate_id"))
        for candidate in selected_candidates
        if candidate.get("candidate_id")
    ]
    evidence_section = _pit_replay_evidence_section(
        pit_replay_evidence,
        source_2438_pit_replay,
    )
    blocker_section = _pit_replay_blocker_section(
        pit_replay_blocker_summary,
        source_2438_pit_replay,
    )
    evidence_rows = [
        row for row in _sequence(evidence_section.get("rows")) if isinstance(row, Mapping)
    ]
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    checks = _remediation_checks(
        source_2440_promotion_review,
        source_2439_forward_pack,
        source_2438_pit_replay,
        evidence_section,
        blocker_section,
        evidence_rows,
        selected_ids,
        data_quality_summary,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_text=research_text,
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in checks
        if requirement["status"] != "PASS"
    ]
    status = READY_STATUS if not gaps else BLOCKED_STATUS
    remediation_ready = status == READY_STATUS
    remaining_engine_blockers = [
        gap
        for gap in gaps
        if str(gap.get("requirement_id")) in REMEDIATION_REQUIREMENT_IDS
    ]
    next_route = NEXT_ROUTE_READY if remediation_ready else NEXT_ROUTE_BLOCKED
    remediation_evidence = _remediation_evidence(
        status,
        source_2440_promotion_review,
        source_2439_forward_pack,
        source_2438_pit_replay,
        selected_ids,
        evidence_rows,
        checks,
        gaps,
    )
    before_after = _before_after_comparison(
        status,
        source_2440_promotion_review,
        source_2439_forward_pack,
        source_2438_pit_replay,
        remediation_evidence,
        gaps,
    )
    remaining_blocker_summary = _remaining_blocker_summary(
        status,
        remaining_engine_blockers,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438A",
        "status": status,
        "readiness_status": status,
        "prior_route": "TRADING-2440_Growth_Tilt_Paper_Shadow_Candidate_Promotion_Review",
        "source_tasks": ["TRADING-2440", "TRADING-2439", "TRADING-2438"],
        "prior_promotion_review_status": source_2440_promotion_review.get("status"),
        "prior_forward_aging_status": source_2439_forward_pack.get("status"),
        "prior_pit_replay_status": source_2438_pit_replay.get("status"),
        "blocked_by_forward_aging_gate": _source_2440_blocked_by_forward_aging(
            source_2440_promotion_review
        ),
        "source_2440_blocked_by_forward_aging_gate": (
            _source_2440_blocked_by_forward_aging(source_2440_promotion_review)
        ),
        "source_2439_blocked_by_pit_replay_gate": (
            source_2439_forward_pack.get("status") == EXPECTED_2439_BLOCKED_STATUS
        ),
        "source_2438_replay_engine_blocked": (
            source_2438_pit_replay.get("status") == EXPECTED_2438_BLOCKED_STATUS
        ),
        "not_no_candidate_status": (
            source_2440_promotion_review.get("status") == EXPECTED_2440_STATUS
            and source_2440_promotion_review.get("paper_shadow_candidate_found")
            is False
        ),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "candidate_selection_resolves": _check_passed(
            checks,
            "candidate_selection_resolves",
        ),
        "top3_candidate_ids_present": _check_passed(
            checks,
            "top3_candidate_ids_present",
        ),
        "pit_replay_artifacts_present": _check_passed(
            checks,
            "pit_replay_artifacts_present",
        ),
        "pit_replay_engine_ready": _pit_replay_engine_ready(source_2438_pit_replay),
        "candidate_pit_replay_engine_available": (
            source_2438_pit_replay.get("candidate_pit_replay_engine_available")
            is True
        ),
        "candidate_replay_input_specs_ready": (
            source_2438_pit_replay.get("candidate_replay_input_specs_ready") is True
        ),
        "pit_replay_evidence_ready": evidence_section.get("pit_replay_evidence_ready")
        is True,
        "pit_replay_evidence_complete": _check_passed(
            checks,
            "pit_replay_evidence_complete",
        ),
        "source_traceability_complete": _check_passed(
            checks,
            "source_traceability_complete",
        ),
        "as_of_boundary_explicit": _check_passed(checks, "as_of_boundary_explicit"),
        "valid_until_boundary_explicit": _check_passed(
            checks,
            "valid_until_boundary_explicit",
        ),
        "outcome_linkage_complete": _check_passed(checks, "outcome_linkage_complete"),
        "forward_aging_handoff_ready": _check_passed(
            checks,
            "forward_aging_handoff_ready",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            checks,
            "registry_catalog_docs_alignment",
        ),
        "remediation_ready": remediation_ready,
        "remediation_gap_count": len(gaps),
        "unresolved_engine_blocker_count": len(remaining_engine_blockers),
        "remaining_blockers": remaining_engine_blockers,
        "selected_candidate_ids": selected_ids,
        "remediation_evidence": remediation_evidence,
        "before_after_comparison": before_after,
        "remaining_blocker_summary": remaining_blocker_summary,
        "no_effect_boundary": no_effect_boundary,
        "requirements": checks,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "pit_replay_executed": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "observe_only": True,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_allowed": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "automatic_execution_allowed": False,
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "Replay engine remediation evidence is ready; rerun the top-3 PIT "
            "replay before regenerating forward aging candidate pack."
            if remediation_ready
            else "Replay engine remediation evidence is incomplete; close the "
            "remaining engine/input/boundary/handoff blockers before PIT recheck."
        ),
    }


def _remediation_checks(
    source_2440: Mapping[str, Any],
    source_2439: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    evidence_section: Mapping[str, Any],
    blocker_section: Mapping[str, Any],
    evidence_rows: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    candidate_selection_resolves = (
        source_2438.get("top3_candidate_selection_ready") is True
        and len(selected_ids) > 0
    )
    top3_ids_present = len(selected_ids) == 3 and len(set(selected_ids)) == 3
    pit_replay_artifacts_present = (
        bool(source_2438.get("status"))
        and bool(evidence_section.get("status"))
        and bool(blocker_section.get("status"))
    )
    evidence_matches_candidates = {
        str(row.get("candidate_id")) for row in evidence_rows if row.get("candidate_id")
    } == set(selected_ids)
    source_traceability_complete = _all_rows_true(
        evidence_rows,
        "source_traceability_verified",
        selected_ids,
    )
    as_of_boundary_explicit = _all_rows_true(
        evidence_rows,
        "as_of_boundary_verified",
        selected_ids,
    )
    valid_until_boundary_explicit = _all_rows_true(
        evidence_rows,
        "valid_until_boundary_verified",
        selected_ids,
    )
    outcome_linkage_complete = _all_rows_true(
        evidence_rows,
        "outcome_linkage_ready",
        selected_ids,
    )
    pit_evidence_complete = (
        evidence_section.get("pit_replay_evidence_ready") is True
        and evidence_section.get("pit_replay_executed") is True
        and int(evidence_section.get("pit_candidates_tested", 0) or 0)
        >= len(selected_ids)
        and int(evidence_section.get("pit_replay_blocked_count", 0) or 0) == 0
        and evidence_matches_candidates
        and all(not _sequence(row.get("blocking_gap_ids")) for row in evidence_rows)
    )
    forward_aging_handoff_ready = (
        source_2438.get("recommended_next_research_task") == EXPECTED_2438_READY_ROUTE
        and int(source_2438.get("pit_candidates_tested", 0) or 0) >= len(selected_ids)
        and int(source_2438.get("pit_replay_blocked_count", 0) or 0) == 0
        and all("pit_replay_passed" in row for row in evidence_rows)
    )
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_remediation(research_text)
    )
    return [
        _requirement(
            "prior_2440_blocked_by_forward_aging_gate",
            _source_2440_blocked_by_forward_aging(source_2440),
            "prior_promotion_review_gate_gap",
            {
                "status": source_2440.get("status"),
                "next_route": source_2440.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "source_2439_forward_aging_artifact_resolves",
            bool(source_2439.get("status")),
            "prior_forward_aging_artifact_gap",
            {"status": source_2439.get("status")},
        ),
        _requirement(
            "source_2438_pit_replay_artifact_resolves",
            bool(source_2438.get("status")),
            "prior_pit_replay_artifact_gap",
            {"status": source_2438.get("status")},
        ),
        _requirement(
            "candidate_selection_resolves",
            candidate_selection_resolves,
            "candidate_selection_gap",
            {
                "top3_candidate_selection_ready": source_2438.get(
                    "top3_candidate_selection_ready"
                ),
                "selected_candidate_count": len(selected_ids),
            },
        ),
        _requirement(
            "top3_candidate_ids_present",
            top3_ids_present,
            "candidate_selection_gap",
            {"selected_candidate_ids": list(selected_ids)},
        ),
        _requirement(
            "pit_replay_artifacts_present",
            pit_replay_artifacts_present,
            "pit_replay_artifact_gap",
            {
                "pit_replay_status": source_2438.get("status"),
                "evidence_status": evidence_section.get("status"),
                "blocker_status": blocker_section.get("status"),
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
            "candidate_pit_replay_engine_available",
            source_2438.get("candidate_pit_replay_engine_available") is True,
            "candidate_pit_replay_engine_gap",
            {
                "current_engine_available": source_2438.get(
                    "candidate_pit_replay_engine_available"
                ),
                "prior_status": source_2438.get("status"),
            },
        ),
        _requirement(
            "candidate_replay_input_specs_ready",
            source_2438.get("candidate_replay_input_specs_ready") is True,
            "candidate_pit_replay_input_gap",
            {
                "candidate_replay_input_specs_ready": source_2438.get(
                    "candidate_replay_input_specs_ready"
                )
            },
        ),
        _requirement(
            "source_traceability_complete",
            source_traceability_complete,
            "candidate_source_traceability_gap",
            {"verified_count": _true_count(evidence_rows, "source_traceability_verified")},
        ),
        _requirement(
            "as_of_boundary_explicit",
            as_of_boundary_explicit,
            "candidate_as_of_boundary_gap",
            {"verified_count": _true_count(evidence_rows, "as_of_boundary_verified")},
        ),
        _requirement(
            "valid_until_boundary_explicit",
            valid_until_boundary_explicit,
            "candidate_valid_until_boundary_gap",
            {
                "verified_count": _true_count(
                    evidence_rows,
                    "valid_until_boundary_verified",
                )
            },
        ),
        _requirement(
            "outcome_linkage_complete",
            outcome_linkage_complete,
            "candidate_outcome_linkage_gap",
            {"ready_count": _true_count(evidence_rows, "outcome_linkage_ready")},
        ),
        _requirement(
            "pit_replay_evidence_complete",
            pit_evidence_complete,
            "pit_replay_evidence_gap",
            {
                "pit_replay_evidence_ready": evidence_section.get(
                    "pit_replay_evidence_ready"
                ),
                "pit_replay_executed": evidence_section.get("pit_replay_executed"),
                "pit_candidates_tested": evidence_section.get("pit_candidates_tested"),
                "pit_replay_blocked_count": evidence_section.get(
                    "pit_replay_blocked_count"
                ),
                "evidence_matches_candidates": evidence_matches_candidates,
            },
        ),
        _requirement(
            "forward_aging_handoff_ready",
            forward_aging_handoff_ready,
            "candidate_to_forward_aging_handoff_gap",
            {
                "next_route": source_2438.get("recommended_next_research_task"),
                "pit_candidates_tested": source_2438.get("pit_candidates_tested"),
                "pit_replay_blocked_count": source_2438.get("pit_replay_blocked_count"),
            },
        ),
        _requirement(
            "registry_catalog_docs_alignment",
            registry_catalog_docs_alignment,
            "registry_catalog_doc_gap",
            {
                "required_report_ids": list(REQUIRED_REPORT_IDS),
                "required_system_flow_references": list(
                    REQUIRED_SYSTEM_FLOW_REFERENCES
                ),
            },
        ),
    ]


def _source_2440_blocked_by_forward_aging(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2440_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2440_ROUTE
    )


def _pit_replay_engine_ready(source_2438: Mapping[str, Any]) -> bool:
    return (
        source_2438.get("candidate_pit_replay_engine_available") is True
        and source_2438.get("candidate_replay_input_specs_ready") is True
    )


def _selected_candidates(source_2438: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    selected = _sequence(source_2438.get("selected_candidates"))
    if not selected:
        selected = _sequence(
            _mapping(source_2438.get("top3_candidate_selection")).get(
                "selected_candidates"
            )
        )
    return [candidate for candidate in selected if isinstance(candidate, Mapping)]


def _pit_replay_evidence_section(
    pit_replay_evidence: Mapping[str, Any],
    source_2438: Mapping[str, Any],
) -> Mapping[str, Any]:
    section = _mapping(pit_replay_evidence.get("pit_replay_evidence"))
    if section:
        return section
    section = _mapping(source_2438.get("pit_replay_evidence"))
    if section:
        return section
    return pit_replay_evidence


def _pit_replay_blocker_section(
    pit_replay_blocker_summary: Mapping[str, Any],
    source_2438: Mapping[str, Any],
) -> Mapping[str, Any]:
    section = _mapping(pit_replay_blocker_summary.get("pit_replay_blocker_summary"))
    if section:
        return section
    section = _mapping(source_2438.get("pit_replay_blocker_summary"))
    if section:
        return section
    return pit_replay_blocker_summary


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _all_rows_true(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    selected_ids: Sequence[str],
) -> bool:
    if not selected_ids or len(rows) < len(selected_ids):
        return False
    by_id = {str(row.get("candidate_id")): row for row in rows}
    return all(
        _mapping(by_id.get(candidate_id)).get(field) is True
        for candidate_id in selected_ids
    )


def _true_count(rows: Sequence[Mapping[str, Any]], field: str) -> int:
    return sum(1 for row in rows if row.get(field) is True)


def _remediation_evidence(
    status: str,
    source_2440: Mapping[str, Any],
    source_2439: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    selected_ids: Sequence[str],
    evidence_rows: Sequence[Mapping[str, Any]],
    checks: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": REMEDIATION_EVIDENCE_SCHEMA_VERSION,
        "status": status,
        "remediation_ready": status == READY_STATUS,
        "prior_promotion_review_status": source_2440.get("status"),
        "prior_forward_aging_status": source_2439.get("status"),
        "prior_pit_replay_status": source_2438.get("status"),
        "blocked_by_forward_aging_gate": _source_2440_blocked_by_forward_aging(
            source_2440
        ),
        "top3_candidate_selection_ready": _check_passed(
            checks,
            "candidate_selection_resolves",
        ),
        "top3_candidate_ids_present": _check_passed(
            checks,
            "top3_candidate_ids_present",
        ),
        "pit_replay_engine_ready": _pit_replay_engine_ready(source_2438),
        "pit_replay_evidence_ready": _check_passed(
            checks,
            "pit_replay_evidence_complete",
        ),
        "source_traceability_complete": _check_passed(
            checks,
            "source_traceability_complete",
        ),
        "as_of_boundary_explicit": _check_passed(checks, "as_of_boundary_explicit"),
        "valid_until_boundary_explicit": _check_passed(
            checks,
            "valid_until_boundary_explicit",
        ),
        "forward_aging_handoff_ready": _check_passed(
            checks,
            "forward_aging_handoff_ready",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            checks,
            "registry_catalog_docs_alignment",
        ),
        "selected_candidate_ids": list(selected_ids),
        "candidate_evidence_rows": list(evidence_rows),
        "remediation_gap_count": len(gaps),
        "remaining_blockers": list(gaps),
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "generated_trading_advice": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _before_after_comparison(
    status: str,
    source_2440: Mapping[str, Any],
    source_2439: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    remediation_evidence: Mapping[str, Any],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    before = {
        "promotion_review_status": source_2440.get("status"),
        "forward_aging_status": source_2439.get("status"),
        "pit_replay_status": source_2438.get("status"),
        "candidate_pit_replay_engine_available": source_2438.get(
            "candidate_pit_replay_engine_available"
        ),
        "candidate_replay_input_specs_ready": source_2438.get(
            "candidate_replay_input_specs_ready"
        ),
        "candidate_source_traceability_manifests_ready": source_2438.get(
            "candidate_source_traceability_manifests_ready"
        ),
        "candidate_as_of_boundary_specs_ready": source_2438.get(
            "candidate_as_of_boundary_specs_ready"
        ),
        "candidate_valid_until_boundary_specs_ready": source_2438.get(
            "candidate_valid_until_boundary_specs_ready"
        ),
        "candidate_outcome_linkage_specs_ready": source_2438.get(
            "candidate_outcome_linkage_specs_ready"
        ),
        "pit_candidates_tested": source_2438.get("pit_candidates_tested"),
        "pit_replay_blocked_count": source_2438.get("pit_replay_blocked_count"),
    }
    after = {
        "remediation_status": status,
        "remediation_ready": status == READY_STATUS,
        "pit_replay_engine_ready": remediation_evidence.get("pit_replay_engine_ready"),
        "pit_replay_evidence_ready": remediation_evidence.get(
            "pit_replay_evidence_ready"
        ),
        "source_traceability_complete": remediation_evidence.get(
            "source_traceability_complete"
        ),
        "as_of_boundary_explicit": remediation_evidence.get("as_of_boundary_explicit"),
        "valid_until_boundary_explicit": remediation_evidence.get(
            "valid_until_boundary_explicit"
        ),
        "forward_aging_handoff_ready": remediation_evidence.get(
            "forward_aging_handoff_ready"
        ),
        "remediation_gap_count": len(gaps),
        "next_route": NEXT_ROUTE_READY if status == READY_STATUS else NEXT_ROUTE_BLOCKED,
    }
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "status": status,
        "before": before,
        "after": after,
        "remaining_blockers": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_blocker_summary(
    status: str,
    remaining_engine_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": REMAINING_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "remaining_blocker_summary_ready": True,
        "unresolved_engine_blocker_count": len(remaining_engine_blockers),
        "remaining_blockers": list(remaining_engine_blockers),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
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


def _check_passed(
    checks: Sequence[Mapping[str, Any]],
    requirement_id: str,
) -> bool:
    return any(
        check.get("requirement_id") == requirement_id
        and check.get("status") == "PASS"
        for check in checks
    )


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    reports = report_registry.get("reports")
    if not isinstance(reports, Sequence) or isinstance(reports, str):
        return False
    present = {
        str(report.get("report_id"))
        for report in reports
        if isinstance(report, Mapping) and report.get("report_id")
    }
    return set(report_ids).issubset(present)


def _contains_all(text: str, references: Sequence[str]) -> bool:
    return all(reference in text for reference in references)


def _docs_cover_remediation(text: str) -> bool:
    required_terms = ("PIT", "replay", "forward aging")
    return all(term in text for term in required_terms)


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, str):
        return value
    return ()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
