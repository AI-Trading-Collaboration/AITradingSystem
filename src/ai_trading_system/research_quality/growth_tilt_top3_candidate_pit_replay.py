from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_top3_candidate_pit_replay.v1"
TOP3_CANDIDATE_SELECTION_SCHEMA_VERSION = "growth_tilt_top3_candidate_selection.v1"
PIT_REPLAY_EVIDENCE_SCHEMA_VERSION = "growth_tilt_top3_candidate_pit_replay_evidence.v1"
PIT_REPLAY_BLOCKER_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_blocker_summary.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = "growth_tilt_top3_candidate_pit_replay_no_effect.v1"

READY_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_READY"
BLOCKED_REPLAY_ENGINE_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
)
BLOCKED_DATA_QUALITY_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_DATA_QUALITY_GATE"
)
BLOCKED_EVIDENCE_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2437_STATUS = "GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY"
EXPECTED_2437_NEXT_ROUTE = "TRADING-2438_Growth_Tilt_Top3_Candidate_PIT_Replay"
EXPECTED_2433_STATUS = "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY"
EXPECTED_2431_STATUS = "GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY"
EXPECTED_2433_CANDIDATE_SET_ID = "false_risk_off_missed_upside_2433"
TOP_CANDIDATE_LIMIT = 3
NEXT_ROUTE = "TRADING-2439_Growth_Tilt_Forward_Aging_Candidate_Pack"
BLOCKED_ROUTE = "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
REPORT_TYPE = "growth_tilt_top3_candidate_pit_replay"
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_regime_slice_attribution_review",
    "growth_tilt_false_risk_off_missed_upside_batch_screen",
    "growth_tilt_existing_candidate_evidence_matrix",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-top3-candidate-pit-replay",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
    "top3_candidate_pit_replay_result.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
    "top3_candidate_selection.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
    "pit_replay_evidence.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
    "pit_replay_blocker_summary.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_top3_candidate_pit_replay.md",
    "docs/research/growth_tilt_top3_candidate_selection.md",
    "docs/research/growth_tilt_top3_candidate_pit_replay_evidence.md",
    "docs/research/growth_tilt_top3_candidate_pit_replay_blocker_summary.md",
    "docs/research/growth_tilt_top3_candidate_pit_replay_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438A_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-pit-replay",
    BLOCKED_REPLAY_ENGINE_STATUS,
    BLOCKED_ROUTE,
)
REPLAY_INPUT_REQUIREMENT_IDS: tuple[str, ...] = (
    "candidate_pit_replay_engine_available",
    "candidate_replay_input_specs_ready",
    "candidate_source_traceability_manifests_ready",
    "candidate_as_of_boundary_specs_ready",
    "candidate_valid_until_boundary_specs_ready",
    "candidate_outcome_linkage_specs_ready",
)


def build_growth_tilt_top3_candidate_pit_replay(
    source_2437_regime_review: Mapping[str, Any],
    source_2433_batch_screen: Mapping[str, Any],
    source_2431_existing_candidate_evidence: Mapping[str, Any],
    candidate_set_2433: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    candidate_pit_replay_engine_available: bool = False,
) -> dict[str, Any]:
    selected_candidates = _select_top_pit_candidates(
        source_2433_batch_screen,
        candidate_set_2433,
    )
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _requirements(
        source_2437_regime_review,
        source_2433_batch_screen,
        source_2431_existing_candidate_evidence,
        candidate_set_2433,
        data_quality_summary,
        selected_candidates,
        candidate_pit_replay_engine_available=candidate_pit_replay_engine_available,
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
    status = _status_from_gaps(gaps)
    selection = _top3_candidate_selection(status, selected_candidates, gaps)
    evidence = _pit_replay_evidence(status, selected_candidates, gaps)
    blocker_summary = _pit_replay_blocker_summary(status, selected_candidates, gaps)
    boundary = _no_effect_boundary(status, gaps)
    replay_blocked = status != READY_STATUS

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2437", "TRADING-2433", "TRADING-2431"],
        "source_2437_ready": _source_2437_ready(source_2437_regime_review),
        "source_2433_batch_screen_ready": _source_2433_ready(source_2433_batch_screen),
        "source_2431_existing_candidate_evidence_ready": _source_2431_ready(
            source_2431_existing_candidate_evidence
        ),
        "candidate_set_2433_ready": _candidate_set_2433_ready(candidate_set_2433),
        "top3_candidate_selection_ready": selection["top3_candidate_selection_ready"],
        "pit_replay_evidence_artifact_ready": evidence["pit_replay_evidence_ready"],
        "pit_replay_blocker_summary_ready": blocker_summary[
            "pit_replay_blocker_summary_ready"
        ],
        "no_effect_boundary_ready": boundary["no_effect_boundary_ready"],
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "pit_candidates_selected": len(selected_candidates),
        "pit_candidates_tested": TOP_CANDIDATE_LIMIT if status == READY_STATUS else 0,
        "pit_replay_pass_count": 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": len(selected_candidates) if replay_blocked else 0,
        "promotion_review_candidate_count": 0,
        "selected_candidates": selected_candidates,
        "candidate_pit_replay_engine_available": candidate_pit_replay_engine_available,
        "candidate_replay_input_specs_ready": False,
        "candidate_source_traceability_manifests_ready": False,
        "candidate_as_of_boundary_specs_ready": False,
        "candidate_valid_until_boundary_specs_ready": False,
        "candidate_outcome_linkage_specs_ready": False,
        "source_traceability_verified_count": 0,
        "as_of_boundary_verified_count": 0,
        "valid_until_boundary_verified_count": 0,
        "outcome_linkage_ready_count": 0,
        "pit_replay_run": status == READY_STATUS,
        "pit_replay_executed": status == READY_STATUS,
        "computed_new_metrics": status == READY_STATUS,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "top3_candidate_selection": selection,
        "pit_replay_evidence": evidence,
        "pit_replay_blocker_summary": blocker_summary,
        "no_effect_boundary": boundary,
        "market_data_experiment_run": status == READY_STATUS,
        "historical_screen_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "cached_market_data_quality_gate_run": True,
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
            "Top-3 PIT replay passed and candidates may move to forward aging."
            if status == READY_STATUS
            else "Growth Tilt candidate-specific PIT replay engine/input specs "
            "are missing; remediate before forward aging."
        ),
    }


def _requirements(
    source_2437: Mapping[str, Any],
    source_2433: Mapping[str, Any],
    source_2431: Mapping[str, Any],
    candidate_set_2433: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    selected_candidates: Sequence[Mapping[str, Any]],
    *,
    candidate_pit_replay_engine_available: bool,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2437_regime_slice_attribution_ready",
            _source_2437_ready(source_2437),
            "prior_regime_slice_attribution_gap",
            {"status": source_2437.get("status")},
        ),
        _requirement(
            "source_2433_batch_screen_ready",
            _source_2433_ready(source_2433),
            "prior_batch_screen_gap",
            {"status": source_2433.get("status")},
        ),
        _requirement(
            "source_2431_existing_candidate_evidence_ready",
            _source_2431_ready(source_2431),
            "prior_candidate_evidence_gap",
            {"status": source_2431.get("status")},
        ),
        _requirement(
            "candidate_set_2433_ready",
            _candidate_set_2433_ready(candidate_set_2433),
            "candidate_set_gap",
            {"candidate_set_id": candidate_set_2433.get("candidate_set_id")},
        ),
        _requirement(
            "top3_pit_candidate_selection_ready",
            len(selected_candidates) == TOP_CANDIDATE_LIMIT,
            "candidate_selection_gap",
            {"selected_candidate_count": len(selected_candidates)},
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
            candidate_pit_replay_engine_available,
            "candidate_pit_replay_engine_gap",
            {
                "required_engine": "growth_tilt_candidate_specific_pit_replay_engine",
                "current_engine": None,
            },
        ),
        _requirement(
            "candidate_replay_input_specs_ready",
            False,
            "candidate_pit_replay_input_gap",
            {"selected_candidate_count": len(selected_candidates)},
        ),
        _requirement(
            "candidate_source_traceability_manifests_ready",
            False,
            "candidate_source_traceability_gap",
            {"selected_candidate_count": len(selected_candidates)},
        ),
        _requirement(
            "candidate_as_of_boundary_specs_ready",
            False,
            "candidate_as_of_boundary_gap",
            {"selected_candidate_count": len(selected_candidates)},
        ),
        _requirement(
            "candidate_valid_until_boundary_specs_ready",
            False,
            "candidate_valid_until_boundary_gap",
            {"selected_candidate_count": len(selected_candidates)},
        ),
        _requirement(
            "candidate_outcome_linkage_specs_ready",
            False,
            "candidate_outcome_linkage_gap",
            {"selected_candidate_count": len(selected_candidates)},
        ),
        _requirement(
            "prior_research_doc_coverage",
            "PIT" in research_text or "pit" in research_text,
            "research_doc_gap",
            {"required_references": ["PIT replay", "pit_candidate"]},
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


def _source_2437_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2437_STATUS
        and payload.get("regime_slice_attribution_review_ready") is True
        and payload.get("recommended_next_research_task") == EXPECTED_2437_NEXT_ROUTE
        and payload.get("regime_attribution_run") is False
    )


def _source_2433_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2433_STATUS
        and payload.get("batch_screen_ready") is True
        and int(payload.get("pit_candidate_count", 0)) >= TOP_CANDIDATE_LIMIT
        and payload.get("candidate_batch_screen_run") is True
    )


def _source_2431_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2431_STATUS
        and payload.get("existing_candidate_evidence_matrix_ready") is True
        and payload.get("candidate_status_summary_ready") is True
    )


def _candidate_set_2433_ready(candidate_set: Mapping[str, Any]) -> bool:
    candidates = _sequence(candidate_set.get("candidates"))
    pit_candidates = [
        candidate
        for candidate in candidates
        if isinstance(candidate, Mapping)
        and candidate.get("default_batch_decision") == "pit_candidate"
    ]
    return (
        candidate_set.get("candidate_set_id") == EXPECTED_2433_CANDIDATE_SET_ID
        and candidate_set.get("status") == "ready"
        and len(pit_candidates) >= TOP_CANDIDATE_LIMIT
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _select_top_pit_candidates(
    source_2433: Mapping[str, Any],
    candidate_set_2433: Mapping[str, Any],
) -> list[dict[str, Any]]:
    matrix_candidates = _sequence(
        _mapping(source_2433.get("candidate_screen_matrix")).get("candidates")
    )
    source_candidates = [
        candidate
        for candidate in matrix_candidates
        if isinstance(candidate, Mapping)
        and candidate.get("batch_decision") == "pit_candidate"
    ]
    if not source_candidates:
        source_candidates = [
            candidate
            for candidate in _sequence(candidate_set_2433.get("candidates"))
            if isinstance(candidate, Mapping)
            and candidate.get("default_batch_decision") == "pit_candidate"
        ]
    selected: list[dict[str, Any]] = []
    for rank, candidate in enumerate(source_candidates[:TOP_CANDIDATE_LIMIT], start=1):
        selected.append(
            {
                "selection_rank": rank,
                "candidate_id": candidate.get("candidate_id"),
                "candidate_family": candidate.get("candidate_family"),
                "batch_decision": candidate.get(
                    "batch_decision",
                    candidate.get("default_batch_decision"),
                ),
                "research_questions": list(
                    _sequence(candidate.get("research_questions"))
                ),
                "pit_replay_status": "blocked_replay_engine_gap",
                "pit_replay_passed": False,
                "promotion_review_candidate": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return selected


def _status_from_gaps(gaps: Sequence[Mapping[str, Any]]) -> str:
    if not gaps:
        return READY_STATUS
    gap_ids = {str(gap.get("requirement_id")) for gap in gaps}
    if "data_quality_gate_passed" in gap_ids:
        return BLOCKED_DATA_QUALITY_STATUS
    if gap_ids.intersection(REPLAY_INPUT_REQUIREMENT_IDS):
        return BLOCKED_REPLAY_ENGINE_STATUS
    return BLOCKED_EVIDENCE_STATUS


def _top3_candidate_selection(
    status: str,
    selected_candidates: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": TOP3_CANDIDATE_SELECTION_SCHEMA_VERSION,
        "status": status,
        "top3_candidate_selection_ready": len(selected_candidates) == TOP_CANDIDATE_LIMIT,
        "candidate_limit": TOP_CANDIDATE_LIMIT,
        "pit_candidates_selected": len(selected_candidates),
        "selected_candidates": list(selected_candidates),
        "selection_basis": "prior_batch_screen_pit_candidate_order_no_market_ranking",
        "evidence_gap_count": len(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _pit_replay_evidence(
    status: str,
    selected_candidates: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = [
        {
            "candidate_id": candidate.get("candidate_id"),
            "pit_replay_status": "blocked_replay_engine_gap"
            if status != READY_STATUS
            else "pass",
            "source_traceability_verified": False,
            "as_of_boundary_verified": False,
            "valid_until_boundary_verified": False,
            "outcome_linkage_ready": False,
            "pit_replay_passed": status == READY_STATUS,
            "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
            "production_effect": "none",
            "broker_action": "none",
        }
        for candidate in selected_candidates
    ]
    return {
        "schema_version": PIT_REPLAY_EVIDENCE_SCHEMA_VERSION,
        "status": status,
        "pit_replay_evidence_ready": True,
        "pit_replay_executed": status == READY_STATUS,
        "pit_candidates_tested": TOP_CANDIDATE_LIMIT if status == READY_STATUS else 0,
        "pit_replay_pass_count": len(rows) if status == READY_STATUS else 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": len(rows) if status != READY_STATUS else 0,
        "rows": rows,
        "production_effect": "none",
        "broker_action": "none",
    }


def _pit_replay_blocker_summary(
    status: str,
    selected_candidates: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": PIT_REPLAY_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "pit_replay_blocker_summary_ready": True,
        "blocked": status != READY_STATUS,
        "blocked_candidate_count": len(selected_candidates) if status != READY_STATUS else 0,
        "blocking_gap_count": len(gaps),
        "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
        "blocking_gap_classifications": {
            str(gap["requirement_id"]): gap["classification"] for gap in gaps
        },
        "next_route": NEXT_ROUTE if status == READY_STATUS else BLOCKED_ROUTE,
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
