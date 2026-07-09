from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_top3_candidate_pit_replay_recheck.v1"
CANDIDATE_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_recheck_evidence.v1"
)
CANDIDATE_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_recheck_summary.v1"
)
REMAINING_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_recheck_remaining_blockers.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_recheck_no_effect.v1"
)

READY_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_READY"
BLOCKED_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
NO_PASSING_CANDIDATE_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_NO_PASSING_CANDIDATE"
)

EXPECTED_2438B_STATUS = "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY"
EXPECTED_2438B_ROUTE = (
    "TRADING-2438C_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck"
)
EXPECTED_2438A_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED"
)
EXPECTED_2438_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"

NEXT_ROUTE_READY = (
    "TRADING-2439A_Growth_Tilt_Forward_Aging_Candidate_Pack_Rebuild_After_PIT_Replay_Recheck"
)
NEXT_ROUTE_NO_PASS = "TRADING-2439A_Growth_Tilt_No_Passing_PIT_Candidate_Evidence_Review"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure"
)
REPORT_TYPE = "growth_tilt_top3_candidate_pit_replay_recheck"

RECHECK_BLOCKERS: tuple[str, ...] = (
    "blocker_closure",
    "top3_candidate_selection",
    "pit_replay_evidence",
    "candidate_replay_outputs",
    "source_traceability",
    "as_of_boundary",
    "valid_until_boundary",
    "outcome_linkage",
    "forward_aging_handoff",
)

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_pit_replay_engine_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay_engine_remediation",
    "growth_tilt_top3_candidate_pit_replay",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-top3-candidate-pit-replay-recheck",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/"
    "pit_replay_recheck_result.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/"
    "candidate_replay_evidence.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/"
    "candidate_replay_summary.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/"
    "remaining_recheck_blocker_summary.json",
    "outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_top3_candidate_pit_replay_recheck.md",
    "docs/research/growth_tilt_top3_candidate_replay_evidence.md",
    "docs/research/growth_tilt_top3_candidate_replay_summary.md",
    "docs/research/growth_tilt_top3_candidate_recheck_remaining_blockers.md",
    "docs/research/growth_tilt_top3_candidate_recheck_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2439A_or_2438D_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-pit-replay-recheck",
    READY_STATUS,
    BLOCKED_STATUS,
    NO_PASSING_CANDIDATE_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_NO_PASS,
    NEXT_ROUTE_BLOCKED,
)


def build_growth_tilt_top3_candidate_pit_replay_recheck(
    source_2438b_blocker_closure: Mapping[str, Any],
    source_2438a_remediation: Mapping[str, Any],
    source_2438_pit_replay: Mapping[str, Any],
    pit_replay_evidence: Mapping[str, Any],
    pit_replay_blocker_summary: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    selected_candidates = _selected_candidates(source_2438_pit_replay)
    selected_ids = _candidate_ids(selected_candidates)
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
    candidate_results = _candidate_replay_results(selected_ids, evidence_rows)
    resolved_as_of = as_of or str(
        source_2438b_blocker_closure.get("as_of")
        or source_2438a_remediation.get("as_of")
        or source_2438_pit_replay.get("as_of")
        or ""
    )
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    checks = _recheck_requirements(
        source_2438b_blocker_closure,
        source_2438a_remediation,
        source_2438_pit_replay,
        evidence_section,
        blocker_section,
        selected_ids,
        candidate_results,
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
    remaining_recheck_blockers = [
        gap for gap in gaps if str(gap.get("blocker_id")) in RECHECK_BLOCKERS
    ]
    pit_replay_recheck_ready = not gaps
    pass_count = len(candidate_results["passing_candidates"])
    fail_count = len(candidate_results["failed_candidates"])
    blocked_count = len(candidate_results["blocked_candidates"])
    if not pit_replay_recheck_ready:
        status = BLOCKED_STATUS
        next_route = NEXT_ROUTE_BLOCKED
    elif pass_count > 0:
        status = READY_STATUS
        next_route = NEXT_ROUTE_READY
    else:
        status = NO_PASSING_CANDIDATE_STATUS
        next_route = NEXT_ROUTE_NO_PASS

    candidate_evidence = _candidate_evidence(
        status,
        selected_ids,
        evidence_rows,
        candidate_results,
        gaps,
    )
    candidate_summary = _candidate_summary(
        status,
        selected_ids,
        candidate_results,
        next_route,
    )
    remaining_summary = _remaining_recheck_blocker_summary(
        status,
        remaining_recheck_blockers,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438C",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": "TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure",
        "prior_blocker_closure_status": source_2438b_blocker_closure.get("status"),
        "prior_remediation_status": source_2438a_remediation.get("status"),
        "prior_pit_replay_status": source_2438_pit_replay.get("status"),
        "source_2438b_blocker_closure_ready": _source_2438b_ready(
            source_2438b_blocker_closure
        ),
        "source_2438a_remediation_blocked": (
            source_2438a_remediation.get("status") == EXPECTED_2438A_STATUS
        ),
        "source_2438_pit_replay_blocked": (
            source_2438_pit_replay.get("status") == EXPECTED_2438_STATUS
        ),
        "not_no_candidate_status": _not_no_candidate(source_2438a_remediation),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "pit_replay_recheck_ready": pit_replay_recheck_ready,
        "pit_replay_engine_ready": _check_passed(checks, "pit_replay_engine_ready"),
        "input_specs_ready": _check_passed(checks, "input_specs_ready"),
        "evidence_completeness_ready": _check_passed(
            checks,
            "evidence_completeness_ready",
        ),
        "source_traceability_ready": _check_passed(checks, "source_traceability_ready"),
        "as_of_boundary_ready": _check_passed(checks, "as_of_boundary_ready"),
        "valid_until_boundary_ready": _check_passed(
            checks,
            "valid_until_boundary_ready",
        ),
        "outcome_linkage_ready": _check_passed(checks, "outcome_linkage_ready"),
        "forward_aging_handoff_ready": _check_passed(
            checks,
            "forward_aging_handoff_ready",
        ),
        "top3_candidate_selection_resolves": _check_passed(
            checks,
            "top3_candidate_selection_resolves",
        ),
        "pit_replay_evidence_exists": _check_passed(
            checks,
            "pit_replay_evidence_exists",
        ),
        "candidate_replay_outputs_complete": _check_passed(
            checks,
            "candidate_replay_outputs_complete",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            checks,
            "registry_catalog_docs_alignment",
        ),
        "top3_candidate_count": len(selected_ids),
        "selected_candidate_ids": selected_ids,
        "candidate_replay_pass_count": pass_count,
        "candidate_replay_fail_count": fail_count,
        "candidate_replay_blocked_count": blocked_count,
        "passing_candidates": candidate_results["passing_candidates"],
        "failed_candidates": candidate_results["failed_candidates"],
        "blocked_candidates": candidate_results["blocked_candidates"],
        "remaining_recheck_blockers": remaining_recheck_blockers,
        "candidate_replay_evidence": candidate_evidence,
        "candidate_replay_summary": candidate_summary,
        "remaining_recheck_blocker_summary": remaining_summary,
        "no_effect_boundary": no_effect_boundary,
        "requirements": checks,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [str(gap["requirement_id"]) for gap in gaps],
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
        "recommended_next_research_task_reason": _next_route_reason(status),
    }


def _recheck_requirements(
    source_2438b: Mapping[str, Any],
    source_2438a: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    evidence_section: Mapping[str, Any],
    blocker_section: Mapping[str, Any],
    selected_ids: Sequence[str],
    candidate_results: Mapping[str, list[dict[str, Any]]],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    candidate_outputs_complete = (
        len(selected_ids) == 3
        and len(candidate_results["blocked_candidates"]) == 0
        and (
            len(candidate_results["passing_candidates"])
            + len(candidate_results["failed_candidates"])
        )
        == len(selected_ids)
        and evidence_section.get("pit_replay_evidence_ready") is True
        and evidence_section.get("pit_replay_executed") is True
    )
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_recheck(research_text)
    )
    return [
        _requirement(
            "source_2438b_blocker_closure_ready",
            _source_2438b_ready(source_2438b),
            "prior_2438b_closure_gap",
            "blocker_closure",
            {
                "status": source_2438b.get("status"),
                "next_route": source_2438b.get("recommended_next_research_task"),
                "blocker_closure_ready": source_2438b.get("blocker_closure_ready"),
                "blocker_count_after": source_2438b.get("blocker_count_after"),
            },
        ),
        _requirement(
            "source_2438a_remediation_artifact_resolves",
            bool(source_2438a.get("status")),
            "prior_2438a_artifact_gap",
            None,
            {"status": source_2438a.get("status")},
        ),
        _requirement(
            "source_2438_pit_replay_artifact_resolves",
            bool(source_2438.get("status")),
            "prior_pit_replay_artifact_gap",
            None,
            {"status": source_2438.get("status")},
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_gap",
            None,
            {
                "data_quality_status": data_quality_summary.get("data_quality_status"),
                "report_path": data_quality_summary.get("data_quality_report_path"),
            },
        ),
        _requirement(
            "pit_replay_engine_ready",
            source_2438b.get("pit_replay_engine_ready") is True,
            "pit_replay_engine_gap",
            "blocker_closure",
            {"pit_replay_engine_ready": source_2438b.get("pit_replay_engine_ready")},
        ),
        _requirement(
            "input_specs_ready",
            source_2438b.get("input_specs_ready") is True,
            "input_specs_gap",
            "blocker_closure",
            {"input_specs_ready": source_2438b.get("input_specs_ready")},
        ),
        _requirement(
            "evidence_completeness_ready",
            source_2438b.get("evidence_completeness_ready") is True,
            "evidence_completeness_gap",
            "blocker_closure",
            {
                "evidence_completeness_ready": source_2438b.get(
                    "evidence_completeness_ready"
                )
            },
        ),
        _requirement(
            "source_traceability_ready",
            source_2438b.get("source_traceability_ready") is True,
            "source_traceability_gap",
            "source_traceability",
            {"source_traceability_ready": source_2438b.get("source_traceability_ready")},
        ),
        _requirement(
            "as_of_boundary_ready",
            source_2438b.get("as_of_boundary_ready") is True,
            "as_of_boundary_gap",
            "as_of_boundary",
            {"as_of_boundary_ready": source_2438b.get("as_of_boundary_ready")},
        ),
        _requirement(
            "valid_until_boundary_ready",
            source_2438b.get("valid_until_boundary_ready") is True,
            "valid_until_boundary_gap",
            "valid_until_boundary",
            {
                "valid_until_boundary_ready": source_2438b.get(
                    "valid_until_boundary_ready"
                )
            },
        ),
        _requirement(
            "outcome_linkage_ready",
            source_2438b.get("outcome_linkage_ready") is True,
            "outcome_linkage_gap",
            "outcome_linkage",
            {"outcome_linkage_ready": source_2438b.get("outcome_linkage_ready")},
        ),
        _requirement(
            "forward_aging_handoff_ready",
            source_2438b.get("forward_aging_handoff_ready") is True,
            "forward_aging_handoff_gap",
            "forward_aging_handoff",
            {
                "forward_aging_handoff_ready": source_2438b.get(
                    "forward_aging_handoff_ready"
                )
            },
        ),
        _requirement(
            "top3_candidate_selection_resolves",
            len(selected_ids) == 3 and len(set(selected_ids)) == 3,
            "candidate_selection_gap",
            "top3_candidate_selection",
            {"selected_candidate_ids": list(selected_ids)},
        ),
        _requirement(
            "pit_replay_evidence_exists",
            bool(evidence_section.get("status"))
            and bool(blocker_section.get("status"))
            and len(_sequence(evidence_section.get("rows"))) > 0,
            "pit_replay_evidence_gap",
            "pit_replay_evidence",
            {
                "evidence_status": evidence_section.get("status"),
                "blocker_status": blocker_section.get("status"),
                "row_count": len(_sequence(evidence_section.get("rows"))),
            },
        ),
        _requirement(
            "candidate_replay_outputs_complete",
            candidate_outputs_complete,
            "candidate_replay_output_gap",
            "candidate_replay_outputs",
            {
                "pit_replay_executed": evidence_section.get("pit_replay_executed"),
                "passing_count": len(candidate_results["passing_candidates"]),
                "failed_count": len(candidate_results["failed_candidates"]),
                "blocked_count": len(candidate_results["blocked_candidates"]),
            },
        ),
        _requirement(
            "registry_catalog_docs_alignment",
            registry_catalog_docs_alignment,
            "registry_catalog_doc_gap",
            None,
            {
                "required_report_ids": list(REQUIRED_REPORT_IDS),
                "required_system_flow_references": list(
                    REQUIRED_SYSTEM_FLOW_REFERENCES
                ),
            },
        ),
    ]


def _candidate_replay_results(
    selected_ids: Sequence[str],
    evidence_rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    rows_by_id = {str(row.get("candidate_id")): row for row in evidence_rows}
    passing: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for candidate_id in selected_ids:
        row = _mapping(rows_by_id.get(candidate_id))
        result = _candidate_result(candidate_id, row)
        if result["replay_outcome"] == "pass":
            passing.append(result)
        elif result["replay_outcome"] == "fail":
            failed.append(result)
        else:
            blocked.append(result)
    return {
        "passing_candidates": passing,
        "failed_candidates": failed,
        "blocked_candidates": blocked,
    }


def _candidate_result(candidate_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
    if not row:
        return {
            "candidate_id": candidate_id,
            "replay_outcome": "blocked",
            "replay_status": "missing_replay_row",
            "blocking_gap_ids": ["missing_replay_row"],
            "production_effect": "none",
            "broker_action": "none",
        }
    status = str(row.get("pit_replay_status", "")).lower()
    blocking_gap_ids = [
        str(gap_id) for gap_id in _sequence(row.get("blocking_gap_ids")) if gap_id
    ]
    if blocking_gap_ids or "blocked" in status:
        outcome = "blocked"
    elif row.get("pit_replay_passed") is True or status in {"pass", "passed"}:
        outcome = "pass"
    elif status in {"fail", "failed", "replay_fail", "no_pass"}:
        outcome = "fail"
    else:
        outcome = "blocked"
        blocking_gap_ids = ["unclassified_replay_status"]
    return {
        "candidate_id": candidate_id,
        "replay_outcome": outcome,
        "replay_status": row.get("pit_replay_status"),
        "pit_replay_passed": row.get("pit_replay_passed"),
        "blocking_gap_ids": blocking_gap_ids,
        "source_traceability_verified": row.get("source_traceability_verified"),
        "as_of_boundary_verified": row.get("as_of_boundary_verified"),
        "valid_until_boundary_verified": row.get("valid_until_boundary_verified"),
        "outcome_linkage_ready": row.get("outcome_linkage_ready"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_evidence(
    status: str,
    selected_ids: Sequence[str],
    evidence_rows: Sequence[Mapping[str, Any]],
    candidate_results: Mapping[str, list[dict[str, Any]]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_EVIDENCE_SCHEMA_VERSION,
        "status": status,
        "pit_replay_recheck_ready": not gaps,
        "selected_candidate_ids": list(selected_ids),
        "source_evidence_rows": list(evidence_rows),
        "passing_candidates": list(candidate_results["passing_candidates"]),
        "failed_candidates": list(candidate_results["failed_candidates"]),
        "blocked_candidates": list(candidate_results["blocked_candidates"]),
        "candidate_replay_pass_count": len(candidate_results["passing_candidates"]),
        "candidate_replay_fail_count": len(candidate_results["failed_candidates"]),
        "candidate_replay_blocked_count": len(candidate_results["blocked_candidates"]),
        "remaining_recheck_blockers": list(gaps),
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "generated_trading_advice": False,
        "portfolio_weight_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_summary(
    status: str,
    selected_ids: Sequence[str],
    candidate_results: Mapping[str, list[dict[str, Any]]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "top3_candidate_count": len(selected_ids),
        "candidate_replay_pass_count": len(candidate_results["passing_candidates"]),
        "candidate_replay_fail_count": len(candidate_results["failed_candidates"]),
        "candidate_replay_blocked_count": len(candidate_results["blocked_candidates"]),
        "passing_candidates": list(candidate_results["passing_candidates"]),
        "failed_candidates": list(candidate_results["failed_candidates"]),
        "blocked_candidates": list(candidate_results["blocked_candidates"]),
        "paper_shadow_candidate_found": False,
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_recheck_blocker_summary(
    status: str,
    remaining_recheck_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": REMAINING_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "remaining_recheck_blocker_summary_ready": True,
        "remaining_recheck_blocker_count": len(remaining_recheck_blockers),
        "remaining_recheck_blockers": list(remaining_recheck_blockers),
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
        "paper_shadow_candidate_found": False,
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


def _source_2438b_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438B_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438B_ROUTE
        and payload.get("blocker_closure_ready") is True
        and _int_or_default(payload.get("blocker_count_after"), -1) == 0
    )


def _not_no_candidate(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438A_STATUS
        and payload.get("paper_shadow_candidate_found") is False
        and payload.get("not_no_candidate_status") is True
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _selected_candidates(source_2438: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    selected = _sequence(source_2438.get("selected_candidates"))
    if not selected:
        selected = _sequence(
            _mapping(source_2438.get("top3_candidate_selection")).get(
                "selected_candidates"
            )
        )
    return [candidate for candidate in selected if isinstance(candidate, Mapping)]


def _candidate_ids(candidates: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(candidate.get("candidate_id"))
        for candidate in candidates
        if candidate.get("candidate_id")
    ]


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


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return (
            "At least one top-3 candidate has complete PIT replay pass evidence; "
            "rebuild the forward-aging candidate pack from recheck outputs."
        )
    if status == NO_PASSING_CANDIDATE_STATUS:
        return (
            "The PIT replay recheck flow is complete, but no top-3 candidate "
            "passed; route to no-passing-candidate evidence review."
        )
    return (
        "The PIT replay recheck evidence is incomplete; close remaining replay "
        "output blockers before forward-aging handoff."
    )


def _requirement(
    requirement_id: str,
    passed: bool,
    classification: str,
    blocker_id: str | None,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "requirement_id": requirement_id,
        "status": "PASS" if passed else "FAIL",
        "classification": classification,
        "evidence": dict(evidence),
        "production_effect": "none",
        "broker_action": "none",
    }
    if blocker_id is not None:
        payload["blocker_id"] = blocker_id
    return payload


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    gap = {
        "requirement_id": requirement.get("requirement_id"),
        "classification": requirement.get("classification"),
        "gap": f"{requirement.get('requirement_id')} did not pass.",
        "evidence": requirement.get("evidence", {}),
        "production_effect": "none",
        "broker_action": "none",
    }
    if requirement.get("blocker_id"):
        gap["blocker_id"] = requirement.get("blocker_id")
    return gap


def _check_passed(
    requirements: Sequence[Mapping[str, Any]],
    requirement_id: str,
) -> bool:
    return any(
        requirement.get("requirement_id") == requirement_id
        and requirement.get("status") == "PASS"
        for requirement in requirements
    )


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    reports = report_registry.get("reports")
    if not isinstance(reports, Sequence) or isinstance(reports, (str, bytes)):
        reports = report_registry.get("report_registry")
    if isinstance(reports, Mapping):
        available = set(reports)
    else:
        available = {
            str(report.get("report_id"))
            for report in _sequence(reports)
            if isinstance(report, Mapping) and report.get("report_id")
        }
    return set(report_ids).issubset(available)


def _contains_all(text: str, needles: Sequence[str]) -> bool:
    return all(needle in text for needle in needles)


def _docs_cover_recheck(text: str) -> bool:
    required_terms = ("PIT", "replay", "handoff")
    return _contains_all(text, required_terms)


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
