from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure.v1"
)
DECISION_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_candidate_recheck_after_remaining_blocker_decision_matrix.v1"
)
FORWARD_AGING_HANDOFF_SCHEMA_VERSION = (
    "growth_tilt_candidate_forward_aging_after_remaining_blocker_handoff_summary.v1"
)
PERSISTENT_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_persistent_candidate_replay_blocker_summary.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_candidate_recheck_after_remaining_blocker_no_effect.v1"
)

READY_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_"
    "BLOCKER_CLOSURE_READY"
)
NO_PASSING_CANDIDATE_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_"
    "BLOCKER_CLOSURE_NO_PASSING_CANDIDATE"
)
BLOCKED_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_"
    "BLOCKER_CLOSURE_BLOCKED"
)

EXPECTED_2438H_STATUS = (
    "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY"
)
EXPECTED_2438H_ROUTE = (
    "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_"
    "Remaining_Blocker_Closure"
)
NEXT_ROUTE_READY = (
    "TRADING-2439A_Growth_Tilt_Forward_Aging_Candidate_Pack_Rebuild_After_"
    "PIT_Replay_Recheck"
)
NEXT_ROUTE_NO_PASS = "TRADING-2439A_Growth_Tilt_No_Passing_PIT_Candidate_Evidence_Review"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_"
    "Escalation"
)
REPORT_TYPE = (
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure"
)

REPLAY_STATUSES: tuple[str, ...] = ("PASS", "FAIL", "BLOCKED")
METRIC_KEYS: tuple[str, ...] = (
    "return_delta_vs_baseline",
    "max_drawdown_delta_vs_baseline",
    "turnover_delta_vs_baseline",
    "false_risk_off_delta",
    "missed_upside_delta",
    "whipsaw_delta",
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-top3-candidate-pit-replay-recheck-after-remaining-blocker-closure",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/"
    "recheck_after_remaining_blocker_closure_result.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/"
    "candidate_pass_fail_blocked_decision_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/"
    "forward_aging_handoff_readiness_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/"
    "persistent_candidate_replay_blocker_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/"
    "no_effect_boundary.json",
    "docs/research/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure.md",
    "docs/research/"
    "growth_tilt_candidate_recheck_after_remaining_blocker_decision_matrix.md",
    "docs/research/"
    "growth_tilt_candidate_forward_aging_after_remaining_blocker_handoff_summary.md",
    "docs/research/growth_tilt_persistent_candidate_replay_blocker_summary.md",
    "docs/research/"
    "growth_tilt_candidate_recheck_after_remaining_blocker_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438J_or_2439A_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-pit-replay-recheck-after-remaining-blocker-closure",
    READY_STATUS,
    NO_PASSING_CANDIDATE_STATUS,
    BLOCKED_STATUS,
    EXPECTED_2438H_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_NO_PASS,
    NEXT_ROUTE_BLOCKED,
)


def build_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure(
    source_2438h_remaining_blocker_closure: Mapping[str, Any],
    replay_recheck_readiness_handoff: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    remaining_blocker_before_after_matrix: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    records = _candidate_output_records(
        candidate_replay_output_records,
        source_2438h_remaining_blocker_closure,
    )
    normalized_records = [_normalize_record(record) for record in records]
    selected_ids = _record_ids(normalized_records)
    handoff = _handoff_section(
        replay_recheck_readiness_handoff,
        source_2438h_remaining_blocker_closure,
    )
    before_after = _before_after_section(
        remaining_blocker_before_after_matrix,
        source_2438h_remaining_blocker_closure,
    )
    handoff_candidates = _handoff_candidates(handoff)
    before_after_rows = _before_after_rows(before_after)
    normalized_records = _merge_remaining_closure_evidence(
        normalized_records,
        handoff_candidates,
        before_after_rows,
    )
    passing_candidates = _records_with_status(normalized_records, "PASS")
    failed_candidates = _records_with_status(normalized_records, "FAIL")
    blocked_candidates = _records_with_status(normalized_records, "BLOCKED")
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _recheck_requirements(
        source_2438h_remaining_blocker_closure,
        handoff,
        before_after,
        data_quality_summary,
        normalized_records,
        selected_ids,
        handoff_candidates,
        before_after_rows,
        passing_candidates,
        failed_candidates,
        blocked_candidates,
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
    candidate_records_recheckable = not gaps
    status = _status_from_records(
        candidate_records_recheckable,
        passing_candidates,
        failed_candidates,
        blocked_candidates,
        normalized_records,
    )
    next_route = _next_route(status)
    pit_recheck_complete = status in {READY_STATUS, NO_PASSING_CANDIDATE_STATUS}
    forward_aging_handoff_ready = status == READY_STATUS
    forward_aging_candidates = (
        list(passing_candidates) if forward_aging_handoff_ready else []
    )
    persistent_blockers = _persistent_candidate_replay_blockers(
        status,
        blocked_candidates,
        gaps,
    )
    decision_matrix = _decision_matrix(
        status,
        normalized_records,
        forward_aging_handoff_ready,
        next_route,
    )
    handoff_summary = _forward_aging_handoff_readiness_summary(
        status,
        forward_aging_handoff_ready,
        forward_aging_candidates,
        blocked_candidates,
        next_route,
    )
    persistent_summary = _persistent_candidate_replay_blocker_summary(
        status,
        persistent_blockers,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps, persistent_blockers)
    resolved_as_of = as_of or str(
        source_2438h_remaining_blocker_closure.get("as_of")
        or _first_record_as_of(normalized_records)
        or ""
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438I",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": (
            "TRADING-2438H_Growth_Tilt_Remaining_Candidate_PIT_Replay_"
            "Blocker_Closure"
        ),
        "prior_status": source_2438h_remaining_blocker_closure.get("status"),
        "source_2438h_remaining_blocker_closure_ready": _source_2438h_ready(
            source_2438h_remaining_blocker_closure
        ),
        "remaining_candidate_blocker_closure_ready": (
            _remaining_blocker_closure_ready(
                source_2438h_remaining_blocker_closure,
                before_after,
            )
        ),
        "remaining_candidate_blocker_count_before": _remaining_count_before(
            source_2438h_remaining_blocker_closure,
            before_after,
        ),
        "remaining_candidate_blocker_count_after": _remaining_count_after(
            source_2438h_remaining_blocker_closure,
            before_after,
        ),
        "replay_recheck_handoff_ready": _replay_recheck_handoff_ready(handoff),
        "candidate_recheckable_after_closure_count": _candidate_recheckable_count(
            source_2438h_remaining_blocker_closure,
            handoff,
            before_after,
        ),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "candidate_replay_outputs_complete": _candidate_outputs_complete(
            source_2438h_remaining_blocker_closure,
            candidate_replay_output_records,
        ),
        "candidate_replay_output_record_count": len(normalized_records),
        "candidate_records_recheckable_after_remaining_blocker_closure": (
            candidate_records_recheckable
        ),
        "pit_replay_recheck_after_remaining_blocker_closure_complete": (
            pit_recheck_complete
        ),
        "candidate_replay_pass_count": len(passing_candidates),
        "candidate_replay_fail_count": len(failed_candidates),
        "candidate_replay_blocked_count": len(blocked_candidates),
        "passing_candidates": passing_candidates,
        "failed_candidates": failed_candidates,
        "blocked_candidates": blocked_candidates,
        "persistent_candidate_replay_blockers": persistent_blockers,
        "persistent_candidate_replay_blocker_count": len(persistent_blockers),
        "forward_aging_handoff_ready": forward_aging_handoff_ready,
        "forward_aging_candidate_count": len(forward_aging_candidates),
        "forward_aging_candidates": forward_aging_candidates,
        "top3_candidate_count": len(selected_ids),
        "top3_candidate_ids": selected_ids,
        "handoff_candidate_count": len(handoff_candidates),
        "before_after_row_count": len(before_after_rows),
        "each_candidate_has_replay_status": _check_passed(
            requirements,
            "each_candidate_has_replay_status",
        ),
        "each_candidate_has_status_reason": _check_passed(
            requirements,
            "each_candidate_has_status_reason",
        ),
        "pass_fail_blocked_counts_consistent": _check_passed(
            requirements,
            "pass_fail_blocked_counts_consistent",
        ),
        "blocked_candidates_have_persistent_blocker_reason": _check_passed(
            requirements,
            "blocked_candidates_have_persistent_blocker_reason",
        ),
        "pass_candidates_have_forward_aging_handoff_key": _check_passed(
            requirements,
            "pass_candidates_have_forward_aging_handoff_key",
        ),
        "forward_aging_handoff_pass_only": _check_passed(
            requirements,
            "forward_aging_handoff_pass_only",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
        "candidate_pass_fail_blocked_decision_matrix": decision_matrix,
        "forward_aging_handoff_readiness_summary": handoff_summary,
        "persistent_candidate_replay_blocker_summary": persistent_summary,
        "no_effect_boundary": no_effect_boundary,
        "requirements": requirements,
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
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "candidate_tracking_started": False,
        "outcome_binding_enabled": False,
        "outcome_binding_executed": False,
        "outcome_backfilled": False,
        "outcome_store_mutated": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "observe_only": True,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_approved": False,
        "paper_shadow_daily_job_run": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "generated_signal": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "daily_report_generated": False,
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
    source_2438h: Mapping[str, Any],
    handoff: Mapping[str, Any],
    before_after: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
    handoff_candidates: Sequence[Mapping[str, Any]],
    before_after_rows: Sequence[Mapping[str, Any]],
    passing_candidates: Sequence[Mapping[str, Any]],
    failed_candidates: Sequence[Mapping[str, Any]],
    blocked_candidates: Sequence[Mapping[str, Any]],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    record_count_ready = len(records) == 3 and len(set(selected_ids)) == 3
    handoff_ids = {
        str(candidate.get("candidate_id"))
        for candidate in handoff_candidates
        if candidate.get("candidate_id")
    }
    before_after_ids = {
        str(row.get("candidate_id"))
        for row in before_after_rows
        if row.get("candidate_id")
    }
    handoff_matches_records = record_count_ready and set(selected_ids) == handoff_ids
    before_after_matches_records = (
        record_count_ready and set(selected_ids) == before_after_ids
    )
    replay_status_ready = _all_records_have_replay_status(records)
    status_reason_ready = _all_records_have_status_reason(records)
    counts_consistent = (
        len(records)
        == len(passing_candidates) + len(failed_candidates) + len(blocked_candidates)
        and replay_status_ready
    )
    blocked_have_reason = all(
        bool(_mapping(record.get("status_reason")).get("blocker_reason"))
        for record in blocked_candidates
    )
    pass_records_ready = all(
        _record_has_metric_summary(record)
        and bool(_mapping(record.get("status_reason")).get("pass_reason"))
        and bool(record.get("evidence_ref"))
        and bool(record.get("forward_aging_handoff_key"))
        and bool(record.get("baseline_id"))
        for record in passing_candidates
    )
    fail_records_ready = all(
        _record_has_metric_summary(record)
        and bool(_mapping(record.get("status_reason")).get("fail_reason"))
        and bool(record.get("failed_criteria"))
        and bool(record.get("baseline_id"))
        for record in failed_candidates
    )
    forward_aging_pass_only = all(
        str(record.get("replay_status", "")).upper() == "PASS"
        for record in passing_candidates
    )
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_recheck(research_text)
    )
    return [
        _requirement(
            "source_2438h_remaining_blocker_closure_ready",
            _source_2438h_ready(source_2438h),
            "prior_2438h_not_ready",
            "remaining_blocker_closure_source_gap",
            {
                "status": source_2438h.get("status"),
                "next_route": source_2438h.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "remaining_candidate_blocker_closure_ready",
            _remaining_blocker_closure_ready(source_2438h, before_after),
            "remaining_blocker_closure_not_complete",
            "remaining_candidate_replay_blocker_still_open",
            {
                "remaining_candidate_blocker_closure_ready": source_2438h.get(
                    "remaining_candidate_blocker_closure_ready"
                ),
                "remaining_candidate_blocker_count_after": _remaining_count_after(
                    source_2438h,
                    before_after,
                ),
            },
        ),
        _requirement(
            "replay_recheck_handoff_ready",
            _replay_recheck_handoff_ready(handoff),
            "replay_recheck_handoff_not_ready",
            "candidate_recheck_handoff_gap",
            {
                "replay_recheck_handoff_ready": handoff.get(
                    "replay_recheck_handoff_ready"
                ),
                "candidate_recheckable_after_closure_count": handoff.get(
                    "candidate_recheckable_after_closure_count"
                ),
            },
        ),
        _requirement(
            "candidate_recheckable_after_closure_count",
            _candidate_recheckable_count(source_2438h, handoff, before_after) == 3,
            "candidate_recheckable_count_not_three",
            "candidate_recheck_handoff_gap",
            {
                "candidate_recheckable_after_closure_count": (
                    _candidate_recheckable_count(source_2438h, handoff, before_after)
                )
            },
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_gap",
            None,
            {"data_quality_status": data_quality_summary.get("data_quality_status")},
        ),
        _requirement(
            "candidate_replay_outputs_complete",
            _candidate_outputs_complete(source_2438h, {}),
            "candidate_output_records_incomplete",
            "candidate_replay_output_record_gap",
            {
                "candidate_output_records_complete": source_2438h.get(
                    "candidate_output_records_complete"
                )
            },
        ),
        _requirement(
            "candidate_replay_output_record_count",
            record_count_ready,
            "candidate_output_record_count_gap",
            "candidate_replay_output_record_gap",
            {"record_ids": list(selected_ids), "record_count": len(records)},
        ),
        _requirement(
            "handoff_candidates_match_records",
            handoff_matches_records,
            "candidate_handoff_record_mismatch",
            "candidate_recheck_handoff_gap",
            {"handoff_ids": sorted(handoff_ids), "record_ids": list(selected_ids)},
        ),
        _requirement(
            "before_after_rows_match_records",
            before_after_matches_records,
            "candidate_before_after_record_mismatch",
            "remaining_blocker_closure_source_gap",
            {
                "before_after_ids": sorted(before_after_ids),
                "record_ids": list(selected_ids),
            },
        ),
        _requirement(
            "each_candidate_has_replay_status",
            replay_status_ready,
            "candidate_replay_status_gap",
            "candidate_replay_output_record_gap",
            {"missing": _missing_replay_status(records)},
        ),
        _requirement(
            "each_candidate_has_status_reason",
            status_reason_ready,
            "candidate_status_reason_gap",
            "candidate_replay_output_record_gap",
            {"missing": _missing_status_reason(records)},
        ),
        _requirement(
            "pass_fail_blocked_counts_consistent",
            counts_consistent,
            "candidate_count_consistency_gap",
            "candidate_replay_output_record_gap",
            {
                "record_count": len(records),
                "pass_count": len(passing_candidates),
                "fail_count": len(failed_candidates),
                "blocked_count": len(blocked_candidates),
            },
        ),
        _requirement(
            "pass_candidates_have_metric_evidence_and_handoff",
            pass_records_ready,
            "pass_candidate_evidence_gap",
            "candidate_replay_output_record_gap",
            {"pass_candidate_count": len(passing_candidates)},
        ),
        _requirement(
            "fail_candidates_have_metric_evidence_and_failed_criteria",
            fail_records_ready,
            "fail_candidate_evidence_gap",
            "candidate_replay_output_record_gap",
            {"fail_candidate_count": len(failed_candidates)},
        ),
        _requirement(
            "blocked_candidates_have_persistent_blocker_reason",
            blocked_have_reason,
            "candidate_replay_blocker_reason_gap",
            "persistent_candidate_replay_blocker",
            {"blocked_candidate_count": len(blocked_candidates)},
        ),
        _requirement(
            "pass_candidates_have_forward_aging_handoff_key",
            all(bool(record.get("forward_aging_handoff_key")) for record in passing_candidates),
            "forward_aging_handoff_key_gap",
            "candidate_replay_output_record_gap",
            {"pass_candidate_count": len(passing_candidates)},
        ),
        _requirement(
            "forward_aging_handoff_pass_only",
            forward_aging_pass_only,
            "forward_aging_handoff_policy_gap",
            None,
            {"pass_candidate_count": len(passing_candidates)},
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


def _candidate_output_records(
    candidate_replay_output_records: Mapping[str, Any],
    source_2438h: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (candidate_replay_output_records, source_2438h):
        section = _mapping(source.get("candidate_replay_output_records"))
        records = _sequence(section.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _handoff_section(
    replay_recheck_readiness_handoff: Mapping[str, Any],
    source_2438h: Mapping[str, Any],
) -> Mapping[str, Any]:
    for source in (replay_recheck_readiness_handoff, source_2438h):
        section = _mapping(source.get("replay_recheck_readiness_handoff"))
        if section:
            return section
    return {}


def _before_after_section(
    remaining_blocker_before_after_matrix: Mapping[str, Any],
    source_2438h: Mapping[str, Any],
) -> Mapping[str, Any]:
    for source in (remaining_blocker_before_after_matrix, source_2438h):
        section = _mapping(source.get("remaining_candidate_blocker_before_after_matrix"))
        if section:
            return section
    return {}


def _handoff_candidates(handoff: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        candidate
        for candidate in _sequence(handoff.get("recheckable_candidates"))
        if isinstance(candidate, Mapping)
    ]


def _before_after_rows(before_after: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        row for row in _sequence(before_after.get("rows")) if isinstance(row, Mapping)
    ]


def _normalize_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["replay_status"] = str(normalized.get("replay_status", "")).upper()
    status_reason = _mapping(normalized.get("status_reason"))
    normalized["status_reason"] = {
        "pass_reason": _optional_string(status_reason.get("pass_reason")),
        "fail_reason": _optional_string(status_reason.get("fail_reason")),
        "blocker_reason": _optional_string(status_reason.get("blocker_reason")),
    }
    if normalized["replay_status"] == "FAIL" and not normalized.get("failed_criteria"):
        fail_reason = normalized["status_reason"].get("fail_reason")
        normalized["failed_criteria"] = [fail_reason] if fail_reason else []
    normalized["paper_shadow_candidate_found"] = False
    normalized["trading_advice_generated"] = False
    normalized["broker_order_generated"] = False
    normalized["portfolio_weight_mutated"] = False
    normalized["production_effect"] = "none"
    normalized["broker_action"] = "none"
    return normalized


def _merge_remaining_closure_evidence(
    records: Sequence[Mapping[str, Any]],
    handoff_candidates: Sequence[Mapping[str, Any]],
    before_after_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    handoff_by_id = {
        str(candidate.get("candidate_id")): candidate
        for candidate in handoff_candidates
        if candidate.get("candidate_id")
    }
    before_after_by_id = {
        str(row.get("candidate_id")): row
        for row in before_after_rows
        if row.get("candidate_id")
    }
    merged: list[dict[str, Any]] = []
    for record in records:
        candidate_id = str(record.get("candidate_id") or "")
        enriched = dict(record)
        handoff = _mapping(handoff_by_id.get(candidate_id))
        before_after = _mapping(before_after_by_id.get(candidate_id))
        if not enriched.get("closure_evidence_ref"):
            enriched["closure_evidence_ref"] = handoff.get("closure_evidence_ref")
        enriched["candidate_recheckable_after_remaining_blocker_closure"] = (
            handoff.get("candidate_id") == candidate_id
            or before_after.get("candidate_recheckable_after_closure") is True
        )
        enriched["remaining_blocker_after_closure"] = before_after.get(
            "remaining_blocker_after_closure"
        )
        enriched["remaining_blocker_closure_result"] = before_after.get(
            "closure_result"
        )
        enriched["replay_outcome_after_remaining_blocker_closure"] = before_after.get(
            "replay_outcome_after_closure"
        )
        merged.append(enriched)
    return merged


def _records_with_status(
    records: Sequence[Mapping[str, Any]],
    replay_status: str,
) -> list[dict[str, Any]]:
    return [
        dict(record)
        for record in records
        if str(record.get("replay_status", "")).upper() == replay_status
    ]


def _status_from_records(
    candidate_records_recheckable: bool,
    passing_candidates: Sequence[Mapping[str, Any]],
    failed_candidates: Sequence[Mapping[str, Any]],
    blocked_candidates: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
) -> str:
    if not candidate_records_recheckable:
        return BLOCKED_STATUS
    if blocked_candidates:
        return BLOCKED_STATUS
    if passing_candidates:
        return READY_STATUS
    if len(records) == 3 and len(failed_candidates) == 3:
        return NO_PASSING_CANDIDATE_STATUS
    return BLOCKED_STATUS


def _next_route(status: str) -> str:
    if status == READY_STATUS:
        return NEXT_ROUTE_READY
    if status == NO_PASSING_CANDIDATE_STATUS:
        return NEXT_ROUTE_NO_PASS
    return NEXT_ROUTE_BLOCKED


def _persistent_candidate_replay_blockers(
    status: str,
    blocked_candidates: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if status != BLOCKED_STATUS:
        return blockers
    for record in blocked_candidates:
        status_reason = _mapping(record.get("status_reason"))
        blockers.append(
            {
                "candidate_id": record.get("candidate_id"),
                "blocker_category": _blocker_categories(record),
                "blocker_reason": status_reason.get("blocker_reason")
                or "Candidate remains BLOCKED after remaining blocker closure.",
                "required_next_action": (
                    "Escalate through TRADING-2438J persistent candidate PIT replay "
                    "blocker review before any forward-aging handoff."
                ),
                "evidence_ref": record.get("evidence_ref"),
                "closure_evidence_ref": record.get("closure_evidence_ref"),
                "remaining_blocker_closure_result": record.get(
                    "remaining_blocker_closure_result"
                ),
                "replay_outcome_after_remaining_blocker_closure": record.get(
                    "replay_outcome_after_remaining_blocker_closure"
                ),
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    if not blockers:
        for gap in gaps:
            blocker_id = gap.get("blocker_id")
            if blocker_id:
                blockers.append(
                    {
                        "candidate_id": None,
                        "blocker_category": [blocker_id],
                        "blocker_reason": gap.get("reason"),
                        "required_next_action": (
                            "Restore required TRADING-2438I source evidence before "
                            "candidate replay recheck can complete."
                        ),
                        "production_effect": "none",
                        "broker_action": "none",
                    }
                )
    return blockers


def _blocker_categories(record: Mapping[str, Any]) -> list[str]:
    gaps = [str(gap).lower() for gap in _sequence(record.get("blocking_gap_ids"))]
    text = " ".join(
        gaps
        + [
            str(record.get("source_replay_status", "")).lower(),
            str(_mapping(record.get("status_reason")).get("blocker_reason", "")).lower(),
        ]
    )
    categories: list[str] = []
    metric_summary = _mapping(record.get("metric_summary"))
    if not metric_summary or all(metric_summary.get(key) is None for key in METRIC_KEYS):
        categories.append("missing_metric_summary")
    if "input" in text:
        categories.append("unresolved_input_dependency")
    if "pit_window" in text or "as_of" in text:
        categories.append("insufficient_pit_window")
    if "source_traceability" in text:
        categories.append("unresolved_source_traceability")
    if "valid_until" in text:
        categories.append("invalid_valid_until_policy")
    if "outcome" in text:
        categories.append("missing_outcome_linkage")
    if not record.get("forward_aging_handoff_key") or "handoff" in text:
        categories.append("missing_forward_aging_handoff_key")
    if "engine" in text or "execution" in text:
        categories.append("replay_engine_execution_gap")
    return categories or ["persistent_candidate_replay_outcome_blocked"]


def _decision_matrix(
    status: str,
    records: Sequence[Mapping[str, Any]],
    forward_aging_handoff_ready: bool,
    next_route: str,
) -> dict[str, Any]:
    rows = []
    for record in records:
        replay_status = str(record.get("replay_status", "")).upper()
        status_reason = _mapping(record.get("status_reason"))
        rows.append(
            {
                "candidate_id": record.get("candidate_id"),
                "replay_status": replay_status,
                "source_replay_status": record.get("source_replay_status"),
                "pass_reason": status_reason.get("pass_reason"),
                "fail_reason": status_reason.get("fail_reason"),
                "failed_criteria": list(_sequence(record.get("failed_criteria"))),
                "persistent_blocker_category": (
                    _blocker_categories(record) if replay_status == "BLOCKED" else []
                ),
                "persistent_blocker_reason": status_reason.get("blocker_reason"),
                "metric_summary": _mapping(record.get("metric_summary")),
                "baseline_id": record.get("baseline_id"),
                "evidence_ref": record.get("evidence_ref"),
                "closure_evidence_ref": record.get("closure_evidence_ref"),
                "source_refs": {
                    "input_spec_ref": record.get("input_spec_ref"),
                    "source_traceability_ref": record.get("source_traceability_ref"),
                    "valid_until_policy_ref": record.get("valid_until_policy_ref"),
                    "outcome_linkage_key": record.get("outcome_linkage_key"),
                },
                "forward_aging_handoff_key": record.get("forward_aging_handoff_key"),
                "forward_aging_eligible": (
                    forward_aging_handoff_ready and replay_status == "PASS"
                ),
                "paper_shadow_candidate_found": False,
                "paper_shadow_enabled": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return {
        "schema_version": DECISION_MATRIX_SCHEMA_VERSION,
        "status": status,
        "decision_matrix_ready": True,
        "rows": rows,
        "candidate_replay_pass_count": sum(
            row["replay_status"] == "PASS" for row in rows
        ),
        "candidate_replay_fail_count": sum(row["replay_status"] == "FAIL" for row in rows),
        "candidate_replay_blocked_count": sum(
            row["replay_status"] == "BLOCKED" for row in rows
        ),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _forward_aging_handoff_readiness_summary(
    status: str,
    forward_aging_handoff_ready: bool,
    forward_aging_candidates: Sequence[Mapping[str, Any]],
    blocked_candidates: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": FORWARD_AGING_HANDOFF_SCHEMA_VERSION,
        "status": status,
        "forward_aging_handoff_ready": forward_aging_handoff_ready,
        "forward_aging_candidate_count": len(forward_aging_candidates),
        "forward_aging_candidates": list(forward_aging_candidates),
        "blocked_candidate_count": len(blocked_candidates),
        "handoff_policy": "PASS_ONLY_AND_NO_BLOCKED_CANDIDATES",
        "next_route": next_route,
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "candidate_tracking_started": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _persistent_candidate_replay_blocker_summary(
    status: str,
    persistent_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": PERSISTENT_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "persistent_candidate_replay_blocker_summary_ready": True,
        "persistent_candidate_replay_blocker_count": len(persistent_blockers),
        "persistent_candidate_replay_blockers": list(persistent_blockers),
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
    persistent_blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "persistent_candidate_replay_blocker_count": len(persistent_blockers),
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
        "pit_replay_executed": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "production_effect": "none",
        "broker_action": "none",
        "gap_count": len(gaps),
        "gap_ids": [str(gap.get("requirement_id")) for gap in gaps],
    }


def _source_2438h_ready(source_2438h: Mapping[str, Any]) -> bool:
    next_route = source_2438h.get("recommended_next_research_task") or source_2438h.get(
        "next_route"
    )
    return (
        source_2438h.get("status") == EXPECTED_2438H_STATUS
        and next_route == EXPECTED_2438H_ROUTE
    )


def _remaining_blocker_closure_ready(
    source_2438h: Mapping[str, Any],
    before_after: Mapping[str, Any],
) -> bool:
    return (
        source_2438h.get("remaining_candidate_blocker_closure_ready") is True
        and _remaining_count_after(source_2438h, before_after) == 0
    )


def _remaining_count_before(
    source_2438h: Mapping[str, Any],
    before_after: Mapping[str, Any],
) -> int:
    before = _mapping(before_after.get("before"))
    return _int(
        source_2438h.get("remaining_candidate_blocker_count_before"),
        _int(before.get("remaining_candidate_blocker_count_before"), 0),
    )


def _remaining_count_after(
    source_2438h: Mapping[str, Any],
    before_after: Mapping[str, Any],
) -> int:
    after = _mapping(before_after.get("after"))
    return _int(
        source_2438h.get("remaining_candidate_blocker_count_after"),
        _int(after.get("remaining_candidate_blocker_count_after"), 0),
    )


def _replay_recheck_handoff_ready(handoff: Mapping[str, Any]) -> bool:
    next_route = handoff.get("next_route")
    return (
        handoff.get("status") == EXPECTED_2438H_STATUS
        and handoff.get("replay_recheck_handoff_ready") is True
        and _int(handoff.get("candidate_recheckable_after_closure_count"), 0) == 3
        and next_route == EXPECTED_2438H_ROUTE
    )


def _candidate_recheckable_count(
    source_2438h: Mapping[str, Any],
    handoff: Mapping[str, Any],
    before_after: Mapping[str, Any],
) -> int:
    after = _mapping(before_after.get("after"))
    return max(
        _int(source_2438h.get("candidate_recheckable_after_closure_count"), 0),
        _int(handoff.get("candidate_recheckable_after_closure_count"), 0),
        _int(after.get("candidate_recheckable_after_closure_count"), 0),
    )


def _candidate_outputs_complete(
    source_2438h: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
) -> bool:
    section = _mapping(candidate_replay_output_records.get("candidate_replay_output_records"))
    return (
        source_2438h.get("candidate_output_records_complete") is True
        or source_2438h.get("candidate_replay_outputs_complete") is True
        or section.get("candidate_replay_output_records_ready") is True
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", "")).upper()
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _all_records_have_replay_status(records: Sequence[Mapping[str, Any]]) -> bool:
    return bool(records) and all(
        str(record.get("replay_status", "")).upper() in REPLAY_STATUSES
        for record in records
    )


def _all_records_have_status_reason(records: Sequence[Mapping[str, Any]]) -> bool:
    return bool(records) and all(_record_has_status_reason(record) for record in records)


def _record_has_status_reason(record: Mapping[str, Any]) -> bool:
    status = str(record.get("replay_status", "")).upper()
    status_reason = _mapping(record.get("status_reason"))
    if status == "PASS":
        return bool(status_reason.get("pass_reason"))
    if status == "FAIL":
        return bool(status_reason.get("fail_reason"))
    if status == "BLOCKED":
        return bool(status_reason.get("blocker_reason"))
    return False


def _record_has_metric_summary(record: Mapping[str, Any]) -> bool:
    metric_summary = _mapping(record.get("metric_summary"))
    return bool(metric_summary) and any(
        metric_summary.get(key) is not None for key in METRIC_KEYS
    )


def _record_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if record.get("candidate_id")
    ]


def _missing_replay_status(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if str(record.get("replay_status", "")).upper() not in REPLAY_STATUSES
    ]


def _missing_status_reason(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if not _record_has_status_reason(record)
    ]


def _first_record_as_of(records: Sequence[Mapping[str, Any]]) -> str | None:
    for record in records:
        if record.get("as_of"):
            return str(record.get("as_of"))
    return None


def _requirement(
    requirement_id: str,
    passed: bool,
    reason: str,
    blocker_id: str | None,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "status": "PASS" if passed else "FAIL",
        "reason": None if passed else reason,
        "blocker_id": blocker_id,
        "evidence": dict(evidence),
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requirement_id": requirement.get("requirement_id"),
        "reason": requirement.get("reason"),
        "blocker_id": requirement.get("blocker_id"),
        "evidence": dict(_mapping(requirement.get("evidence"))),
        "production_effect": "none",
        "broker_action": "none",
    }


def _check_passed(requirements: Sequence[Mapping[str, Any]], requirement_id: str) -> bool:
    return any(
        requirement.get("requirement_id") == requirement_id
        and requirement.get("status") == "PASS"
        for requirement in requirements
    )


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    entries = _sequence(report_registry.get("reports"))
    available = {
        str(entry.get("report_id"))
        for entry in entries
        if isinstance(entry, Mapping) and entry.get("report_id")
    }
    return all(report_id in available for report_id in report_ids)


def _contains_all(text: str, references: Sequence[str]) -> bool:
    return all(reference in text for reference in references)


def _docs_cover_recheck(text: str) -> bool:
    lowered = text.lower()
    return (
        "2438i" in lowered
        and "pass" in lowered
        and "fail" in lowered
        and "blocked" in lowered
        and "remaining" in lowered
        and "forward-aging" in lowered
    )


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return (
            "At least one candidate passed PIT replay after remaining blocker "
            "closure, with no blocked candidates; proceed to forward-aging "
            "candidate pack rebuild."
        )
    if status == NO_PASSING_CANDIDATE_STATUS:
        return (
            "All three candidates are explicit FAIL and none remains BLOCKED; "
            "proceed to no-passing PIT candidate evidence review."
        )
    return (
        "At least one candidate or source requirement remains BLOCKED after "
        "remaining blocker closure; escalate persistent candidate PIT replay "
        "blockers before forward-aging."
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return ()


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
