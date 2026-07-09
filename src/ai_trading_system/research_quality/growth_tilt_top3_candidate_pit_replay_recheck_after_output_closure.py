from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure.v1"
CANDIDATE_LEVEL_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_candidate_level_replay_blocker_summary.v1"
)
DECISION_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_candidate_pass_fail_blocked_decision_matrix.v1"
)
FORWARD_AGING_HANDOFF_SCHEMA_VERSION = (
    "growth_tilt_candidate_forward_aging_handoff_gate.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_candidate_recheck_after_output_no_effect.v1"
)

READY_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_READY"
)
BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_"
    "BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS"
)
NO_PASSING_CANDIDATE_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_"
    "NO_PASSING_CANDIDATE"
)
BLOCKED_BY_OUTPUT_INCOMPLETENESS_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_"
    "BLOCKED_BY_OUTPUT_INCOMPLETENESS"
)

EXPECTED_2438D_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY"
)
EXPECTED_2438D_ROUTE = (
    "TRADING-2438E_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Output_Closure"
)
EXPECTED_2438C_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
EXPECTED_2438B_STATUS = "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY"
EXPECTED_2438B_ROUTE = (
    "TRADING-2438C_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck"
)
NEXT_ROUTE_CANDIDATE_BLOCKERS = (
    "TRADING-2438F_Growth_Tilt_Top3_Candidate_Level_PIT_Replay_Blocker_Closure"
)
NEXT_ROUTE_READY = (
    "TRADING-2439A_Growth_Tilt_Forward_Aging_Candidate_Pack_Rebuild_After_"
    "PIT_Replay_Recheck"
)
NEXT_ROUTE_NO_PASS = "TRADING-2439A_Growth_Tilt_No_Passing_PIT_Candidate_Evidence_Review"
NEXT_ROUTE_OUTPUT_INCOMPLETE = (
    "TRADING-2438E_Growth_Tilt_Replay_Output_Incompleteness_Remaining_"
    "Blocker_Closure"
)
REPORT_TYPE = "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure"

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
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay_recheck",
    "growth_tilt_pit_replay_engine_blocker_closure",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-top3-candidate-pit-replay-recheck-after-output-closure",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure/"
    "recheck_after_output_closure_result.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure/"
    "candidate_level_blocker_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure/"
    "pass_fail_blocked_decision_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure/"
    "forward_aging_handoff_gate.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure/"
    "no_effect_boundary.json",
    "docs/research/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure.md",
    "docs/research/growth_tilt_candidate_level_replay_blocker_summary.md",
    "docs/research/growth_tilt_candidate_pass_fail_blocked_decision_matrix.md",
    "docs/research/growth_tilt_candidate_forward_aging_handoff_gate.md",
    "docs/research/growth_tilt_candidate_recheck_after_output_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438F_or_2439A_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-pit-replay-recheck-after-output-closure",
    READY_STATUS,
    BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS,
    NO_PASSING_CANDIDATE_STATUS,
    BLOCKED_BY_OUTPUT_INCOMPLETENESS_STATUS,
    NEXT_ROUTE_CANDIDATE_BLOCKERS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_NO_PASS,
)


def build_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure(
    source_2438d_output_closure: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    source_2438c_recheck: Mapping[str, Any],
    source_2438b_blocker_closure: Mapping[str, Any],
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
        source_2438d_output_closure,
    )
    selected_ids = _record_ids(records)
    normalized_records = [_normalize_record(record) for record in records]
    passing_candidates = _records_with_status(normalized_records, "PASS")
    failed_candidates = _records_with_status(normalized_records, "FAIL")
    blocked_candidates = _records_with_status(normalized_records, "BLOCKED")
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _recheck_requirements(
        source_2438d_output_closure,
        source_2438c_recheck,
        source_2438b_blocker_closure,
        data_quality_summary,
        normalized_records,
        selected_ids,
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
    outputs_recheckable = not gaps
    status = _status_from_records(
        outputs_recheckable,
        passing_candidates,
        failed_candidates,
        blocked_candidates,
        normalized_records,
    )
    next_route = _next_route(status)
    pit_recheck_ready = status in {READY_STATUS, NO_PASSING_CANDIDATE_STATUS}
    forward_aging_handoff_ready = status == READY_STATUS
    forward_aging_candidates = (
        list(passing_candidates) if forward_aging_handoff_ready else []
    )
    candidate_level_blockers = _candidate_level_blockers(blocked_candidates)
    remaining_recheck_blockers = _remaining_recheck_blockers(
        status,
        candidate_level_blockers,
        gaps,
    )
    decision_matrix = _decision_matrix(
        status,
        normalized_records,
        forward_aging_handoff_ready,
        next_route,
    )
    blocker_summary = _candidate_level_blocker_summary(
        status,
        candidate_level_blockers,
        next_route,
    )
    handoff_gate = _forward_aging_handoff_gate(
        status,
        forward_aging_handoff_ready,
        forward_aging_candidates,
        blocked_candidates,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps, candidate_level_blockers)
    resolved_as_of = as_of or str(
        source_2438d_output_closure.get("as_of")
        or source_2438c_recheck.get("as_of")
        or source_2438b_blocker_closure.get("as_of")
        or ""
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438E",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": (
            "TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_"
            "Blocker_Closure"
        ),
        "prior_status": source_2438d_output_closure.get("status"),
        "source_2438d_output_closure_ready": _source_2438d_ready(
            source_2438d_output_closure
        ),
        "source_2438c_recheck_blocked": _source_2438c_blocked(source_2438c_recheck),
        "source_2438b_blocker_closure_ready": _source_2438b_ready(
            source_2438b_blocker_closure
        ),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "candidate_replay_outputs_complete": _candidate_outputs_complete(
            source_2438d_output_closure
        ),
        "candidate_replay_output_record_count": len(normalized_records),
        "candidate_output_records_recheckable": outputs_recheckable,
        "pit_replay_recheck_after_output_closure_ready": pit_recheck_ready,
        "candidate_replay_pass_count": len(passing_candidates),
        "candidate_replay_fail_count": len(failed_candidates),
        "candidate_replay_blocked_count": len(blocked_candidates),
        "passing_candidates": passing_candidates,
        "failed_candidates": failed_candidates,
        "blocked_candidates": blocked_candidates,
        "candidate_level_blocker_count": len(candidate_level_blockers),
        "candidate_level_blockers": candidate_level_blockers,
        "remaining_recheck_blockers": remaining_recheck_blockers,
        "forward_aging_handoff_ready": forward_aging_handoff_ready,
        "forward_aging_candidate_count": len(forward_aging_candidates),
        "forward_aging_candidates": forward_aging_candidates,
        "top3_candidate_count": len(selected_ids),
        "top3_candidate_ids": selected_ids,
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
        "blocked_candidates_have_blocker_reason": _check_passed(
            requirements,
            "blocked_candidates_have_blocker_reason",
        ),
        "forward_aging_handoff_pass_only": _check_passed(
            requirements,
            "forward_aging_handoff_pass_only",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
        "candidate_level_blocker_summary": blocker_summary,
        "pass_fail_blocked_decision_matrix": decision_matrix,
        "forward_aging_handoff_gate": handoff_gate,
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
    source_2438d: Mapping[str, Any],
    source_2438c: Mapping[str, Any],
    source_2438b: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
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
            "source_2438d_output_closure_ready",
            _source_2438d_ready(source_2438d),
            "prior_2438d_gap",
            "candidate_replay_output_incompleteness",
            {
                "status": source_2438d.get("status"),
                "next_route": source_2438d.get("recommended_next_research_task"),
                "blocker_closure_ready": source_2438d.get("blocker_closure_ready"),
            },
        ),
        _requirement(
            "source_2438c_recheck_blocked",
            _source_2438c_blocked(source_2438c),
            "prior_2438c_gap",
            None,
            {"status": source_2438c.get("status")},
        ),
        _requirement(
            "source_2438b_blocker_closure_ready",
            _source_2438b_ready(source_2438b),
            "prior_2438b_gap",
            None,
            {"status": source_2438b.get("status")},
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
            _candidate_outputs_complete(source_2438d),
            "candidate_output_completeness_gap",
            "candidate_replay_output_incompleteness",
            {
                "candidate_replay_outputs_complete": source_2438d.get(
                    "candidate_replay_outputs_complete"
                ),
            },
        ),
        _requirement(
            "candidate_replay_output_record_count",
            record_count_ready,
            "candidate_output_record_count_gap",
            "candidate_replay_output_incompleteness",
            {"record_ids": list(selected_ids), "record_count": len(records)},
        ),
        _requirement(
            "each_candidate_has_replay_status",
            replay_status_ready,
            "candidate_replay_status_gap",
            "candidate_replay_output_incompleteness",
            {"missing": _missing_replay_status(records)},
        ),
        _requirement(
            "each_candidate_has_status_reason",
            status_reason_ready,
            "candidate_status_reason_gap",
            "candidate_replay_output_incompleteness",
            {"missing": _missing_status_reason(records)},
        ),
        _requirement(
            "pass_fail_blocked_counts_consistent",
            counts_consistent,
            "candidate_count_consistency_gap",
            "candidate_replay_output_incompleteness",
            {
                "record_count": len(records),
                "pass_count": len(passing_candidates),
                "fail_count": len(failed_candidates),
                "blocked_count": len(blocked_candidates),
            },
        ),
        _requirement(
            "blocked_candidates_have_blocker_reason",
            blocked_have_reason,
            "candidate_level_blocker_reason_gap",
            "candidate_level_replay_blockers",
            {"blocked_candidate_count": len(blocked_candidates)},
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
    source_2438d: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (candidate_replay_output_records, source_2438d):
        section = _mapping(source.get("candidate_replay_output_records"))
        records = _sequence(section.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _normalize_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["replay_status"] = str(normalized.get("replay_status", "")).upper()
    status_reason = _mapping(normalized.get("status_reason"))
    normalized["status_reason"] = {
        "pass_reason": _optional_string(status_reason.get("pass_reason")),
        "fail_reason": _optional_string(status_reason.get("fail_reason")),
        "blocker_reason": _optional_string(status_reason.get("blocker_reason")),
    }
    normalized["paper_shadow_candidate_found"] = False
    normalized["trading_advice_generated"] = False
    normalized["broker_order_generated"] = False
    normalized["portfolio_weight_mutated"] = False
    normalized["production_effect"] = "none"
    normalized["broker_action"] = "none"
    return normalized


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
    outputs_recheckable: bool,
    passing_candidates: Sequence[Mapping[str, Any]],
    failed_candidates: Sequence[Mapping[str, Any]],
    blocked_candidates: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
) -> str:
    if not outputs_recheckable:
        return BLOCKED_BY_OUTPUT_INCOMPLETENESS_STATUS
    if blocked_candidates:
        return BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS
    if passing_candidates:
        return READY_STATUS
    if len(records) == 3 and len(failed_candidates) == 3:
        return NO_PASSING_CANDIDATE_STATUS
    return BLOCKED_BY_OUTPUT_INCOMPLETENESS_STATUS


def _next_route(status: str) -> str:
    if status == READY_STATUS:
        return NEXT_ROUTE_READY
    if status == NO_PASSING_CANDIDATE_STATUS:
        return NEXT_ROUTE_NO_PASS
    if status == BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS:
        return NEXT_ROUTE_CANDIDATE_BLOCKERS
    return NEXT_ROUTE_OUTPUT_INCOMPLETE


def _candidate_level_blockers(
    blocked_candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for record in blocked_candidates:
        status_reason = _mapping(record.get("status_reason"))
        categories = _blocker_categories(record)
        blockers.append(
            {
                "candidate_id": record.get("candidate_id"),
                "replay_status": "BLOCKED",
                "blocker_category": categories,
                "blocker_reason": status_reason.get("blocker_reason")
                or "Candidate replay output remains BLOCKED.",
                "required_next_action": (
                    "Close TRADING-2438F candidate-level PIT replay blockers "
                    "before forward-aging handoff."
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
    return categories or ["other"]


def _candidate_level_blocker_summary(
    status: str,
    candidate_level_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_LEVEL_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "candidate_level_blocker_summary_ready": True,
        "candidate_level_blocker_count": len(candidate_level_blockers),
        "candidate_level_blockers": list(candidate_level_blockers),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


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
                "pass_reason": status_reason.get("pass_reason"),
                "fail_reason": status_reason.get("fail_reason"),
                "blocker_reason": status_reason.get("blocker_reason"),
                "forward_aging_eligible": (
                    forward_aging_handoff_ready and replay_status == "PASS"
                ),
                "paper_shadow_candidate_found": False,
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


def _forward_aging_handoff_gate(
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


def _remaining_recheck_blockers(
    status: str,
    candidate_level_blockers: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if status == BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS:
        return [
            {
                "blocker_id": "candidate_level_replay_blockers",
                "candidate_level_blocker_count": len(candidate_level_blockers),
                "production_effect": "none",
                "broker_action": "none",
            }
        ]
    if status == BLOCKED_BY_OUTPUT_INCOMPLETENESS_STATUS:
        return [
            dict(gap)
            for gap in gaps
            if gap.get("blocker_id") == "candidate_replay_output_incompleteness"
        ]
    return []


def _no_effect_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
    candidate_level_blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "candidate_level_blocker_count": len(candidate_level_blockers),
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


def _source_2438d_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438D_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438D_ROUTE
        and payload.get("blocker_closure_ready") is True
        and _candidate_outputs_complete(payload)
    )


def _source_2438c_blocked(payload: Mapping[str, Any]) -> bool:
    return payload.get("status") == EXPECTED_2438C_STATUS


def _source_2438b_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438B_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438B_ROUTE
        and payload.get("blocker_closure_ready") is True
        and _int_or_default(payload.get("blocker_count_after"), -1) == 0
    )


def _candidate_outputs_complete(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("candidate_replay_outputs_complete") is True
        and _int_or_default(payload.get("candidate_replay_output_record_count"), 0) == 3
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _record_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if record.get("candidate_id")
    ]


def _all_records_have_replay_status(records: Sequence[Mapping[str, Any]]) -> bool:
    return all(
        str(record.get("replay_status", "")).upper() in REPLAY_STATUSES
        for record in records
    )


def _all_records_have_status_reason(records: Sequence[Mapping[str, Any]]) -> bool:
    for record in records:
        replay_status = str(record.get("replay_status", "")).upper()
        status_reason = _mapping(record.get("status_reason"))
        if replay_status == "PASS" and not status_reason.get("pass_reason"):
            return False
        if replay_status == "FAIL" and not status_reason.get("fail_reason"):
            return False
        if replay_status == "BLOCKED" and not status_reason.get("blocker_reason"):
            return False
        if replay_status not in REPLAY_STATUSES:
            return False
    return bool(records)


def _missing_replay_status(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if str(record.get("replay_status", "")).upper() not in REPLAY_STATUSES
    ]


def _missing_status_reason(records: Sequence[Mapping[str, Any]]) -> list[str]:
    missing: list[str] = []
    for record in records:
        replay_status = str(record.get("replay_status", "")).upper()
        status_reason = _mapping(record.get("status_reason"))
        if replay_status == "PASS" and not status_reason.get("pass_reason"):
            missing.append(str(record.get("candidate_id")))
        elif replay_status == "FAIL" and not status_reason.get("fail_reason"):
            missing.append(str(record.get("candidate_id")))
        elif replay_status == "BLOCKED" and not status_reason.get("blocker_reason"):
            missing.append(str(record.get("candidate_id")))
        elif replay_status not in REPLAY_STATUSES:
            missing.append(str(record.get("candidate_id")))
    return missing


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return (
            "At least one candidate has PASS output and no candidate remains "
            "BLOCKED; route to forward-aging candidate pack rebuild."
        )
    if status == NO_PASSING_CANDIDATE_STATUS:
        return (
            "All candidate outputs are complete and none is BLOCKED, but no "
            "candidate passed PIT replay."
        )
    if status == BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS_STATUS:
        return (
            "Candidate outputs are complete, but at least one candidate remains "
            "BLOCKED by candidate-level replay blockers."
        )
    return "Candidate output records are not complete enough for after-closure recheck."


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
    required_terms = ("PIT", "replay", "candidate", "blocker")
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


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None
