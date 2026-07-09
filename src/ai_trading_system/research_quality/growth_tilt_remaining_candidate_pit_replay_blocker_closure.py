from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_remaining_candidate_pit_replay_blocker_closure.v1"
CLOSURE_RECORDS_SCHEMA_VERSION = (
    "growth_tilt_remaining_candidate_replay_blocker_closure_records.v1"
)
BEFORE_AFTER_SCHEMA_VERSION = (
    "growth_tilt_remaining_candidate_replay_blocker_before_after.v1"
)
REPLAY_RECHECK_HANDOFF_SCHEMA_VERSION = (
    "growth_tilt_replay_recheck_readiness_handoff.v1"
)
UNRESOLVED_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_unresolved_remaining_candidate_replay_blockers.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure_no_effect.v1"
)

READY_STATUS = "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_BLOCKED"
)

EXPECTED_2438G_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_"
    "BLOCKER_CLOSURE_BLOCKED"
)
EXPECTED_2438G_ROUTE = (
    "TRADING-2438H_Growth_Tilt_Remaining_Candidate_PIT_Replay_Blocker_Closure"
)
EXPECTED_2438F_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY"
)
NEXT_ROUTE_READY = (
    "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_"
    "Remaining_Blocker_Closure"
)
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438I_Growth_Tilt_Remaining_Candidate_PIT_Replay_Blocker_"
    "Closure_Continuation"
)
REPORT_TYPE = "growth_tilt_remaining_candidate_pit_replay_blocker_closure"

ALLOWED_REMAINING_BLOCKER_CATEGORIES: tuple[str, ...] = (
    "unresolved_replay_execution_result",
    "missing_candidate_metric_materialization",
    "missing_candidate_baseline_comparison",
    "missing_candidate_replay_window_evidence",
    "missing_candidate_pass_fail_threshold",
    "missing_candidate_failure_reason_rule",
    "missing_candidate_outcome_linkage_materialization",
    "missing_forward_aging_handoff_materialization",
    "unresolved_candidate_evidence_ref",
    "unresolved_candidate_data_boundary",
    "other",
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure",
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-remaining-candidate-pit-replay-blocker-closure",
    "outputs/research_strategies/"
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure/"
    "blocker_closure_result.json",
    "outputs/research_strategies/"
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure/"
    "remaining_candidate_blocker_closure_records.json",
    "outputs/research_strategies/"
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure/"
    "remaining_candidate_blocker_before_after_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure/"
    "replay_recheck_readiness_handoff.json",
    "outputs/research_strategies/"
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure/"
    "unresolved_remaining_candidate_blocker_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_remaining_candidate_pit_replay_blocker_closure.md",
    "docs/research/"
    "growth_tilt_remaining_candidate_replay_blocker_closure_records.md",
    "docs/research/growth_tilt_remaining_candidate_replay_blocker_before_after.md",
    "docs/research/growth_tilt_replay_recheck_readiness_handoff.md",
    "docs/research/growth_tilt_unresolved_remaining_candidate_replay_blockers.md",
    "docs/research/"
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438I_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-remaining-candidate-pit-replay-blocker-closure",
    READY_STATUS,
    BLOCKED_STATUS,
    EXPECTED_2438G_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_BLOCKED,
    "NOT_RECHECKED",
)


def build_growth_tilt_remaining_candidate_pit_replay_blocker_closure(
    source_2438g_blocked_recheck: Mapping[str, Any],
    source_2438f_candidate_level_blocker_closure: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    remaining_candidate_replay_blocker_summary: Mapping[str, Any],
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
        source_2438g_blocked_recheck,
    )
    normalized_records = [_normalize_candidate_record(record) for record in records]
    blocked_records = _records_with_status(normalized_records, "BLOCKED")
    blocker_records = _remaining_blocker_records(
        remaining_candidate_replay_blocker_summary,
        source_2438g_blocked_recheck,
    )
    blockers_by_id = _records_by_candidate_id(blocker_records)
    source_2438f_records = _candidate_level_closure_records(
        source_2438f_candidate_level_blocker_closure
    )
    source_2438f_by_id = _records_by_candidate_id(source_2438f_records)
    closure_records = [
        _closure_record(
            record,
            blockers_by_id.get(str(record.get("candidate_id"))),
            source_2438f_by_id.get(str(record.get("candidate_id"))),
        )
        for record in blocked_records
    ]
    remaining_count_before = len(blocker_records)
    unresolved_after = [
        record for record in closure_records if record.get("closure_result") != "CLOSED"
    ]
    remaining_count_after = len(unresolved_after)
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _closure_requirements(
        source_2438g_blocked_recheck,
        source_2438f_candidate_level_blocker_closure,
        candidate_replay_output_records,
        data_quality_summary,
        normalized_records,
        blocked_records,
        blocker_records,
        closure_records,
        remaining_count_before,
        remaining_count_after,
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
    closure_ready = not gaps and remaining_count_after == 0
    status = READY_STATUS if closure_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if closure_ready else NEXT_ROUTE_BLOCKED
    closed_blockers = [
        _closed_blocker_summary(record)
        for record in closure_records
        if record.get("closure_result") == "CLOSED"
    ]
    unresolved_blockers = [
        _unresolved_blocker_summary(record)
        for record in closure_records
        if record.get("closure_result") != "CLOSED"
    ]
    closure_records_section = _closure_records_section(
        status,
        closure_records,
        remaining_count_before,
        remaining_count_after,
        next_route,
    )
    before_after_matrix = _before_after_matrix(
        status,
        closure_records,
        remaining_count_before,
        remaining_count_after,
        next_route,
    )
    handoff = _replay_recheck_readiness_handoff(
        status,
        closure_ready,
        closure_records,
        next_route,
    )
    unresolved_summary = _unresolved_remaining_blocker_summary(
        status,
        unresolved_blockers,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps, remaining_count_after)
    resolved_as_of = as_of or str(
        source_2438g_blocked_recheck.get("as_of")
        or _first_record_as_of(normalized_records)
        or ""
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438H",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": (
            "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_"
            "After_Candidate_Blocker_Closure"
        ),
        "prior_status": source_2438g_blocked_recheck.get("status"),
        "prior_candidate_replay_pass_count": int(
            source_2438g_blocked_recheck.get("candidate_replay_pass_count") or 0
        ),
        "prior_candidate_replay_fail_count": int(
            source_2438g_blocked_recheck.get("candidate_replay_fail_count") or 0
        ),
        "prior_candidate_replay_blocked_count": int(
            source_2438g_blocked_recheck.get("candidate_replay_blocked_count") or 0
        ),
        "source_2438g_blocked_recheck_ready": _source_2438g_ready(
            source_2438g_blocked_recheck
        ),
        "source_2438f_candidate_level_closure_ready": _source_2438f_ready(
            source_2438f_candidate_level_blocker_closure
        ),
        "candidate_output_records_complete": _candidate_outputs_complete(
            source_2438g_blocked_recheck,
            candidate_replay_output_records,
        ),
        "candidate_replay_output_record_count": len(normalized_records),
        "replayability_handoff_ready": _replayability_handoff_ready(
            source_2438g_blocked_recheck,
            source_2438f_candidate_level_blocker_closure,
        ),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "remaining_blocker_records_present": bool(blocker_records),
        "remaining_blocker_record_count": len(blocker_records),
        "remaining_candidate_blocker_closure_records_complete": _check_passed(
            requirements,
            "remaining_candidate_blocker_closure_records_complete",
        ),
        "remaining_candidate_blocker_closure_ready": closure_ready,
        "remaining_candidate_blocker_count_before": remaining_count_before,
        "remaining_candidate_blocker_count_after": remaining_count_after,
        "closed_remaining_candidate_blockers": closed_blockers,
        "unresolved_remaining_candidate_blockers": unresolved_blockers,
        "candidate_recheckable_after_closure_count": sum(
            record.get("candidate_recheckable_after_closure") is True
            for record in closure_records
        ),
        "replay_recheck_handoff_ready": closure_ready,
        "candidate_replay_pass_count": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": len(blocked_records),
        "forward_aging_handoff_ready": False,
        "forward_aging_candidate_count": 0,
        "top3_candidate_count": len(_record_ids(normalized_records)),
        "top3_candidate_ids": _record_ids(normalized_records),
        "candidate_level_closure_record_count": len(source_2438f_records),
        "each_blocked_candidate_has_remaining_blocker_reason": _check_passed(
            requirements,
            "each_blocked_candidate_has_remaining_blocker_reason",
        ),
        "each_blocked_candidate_has_closure_action": _check_passed(
            requirements,
            "each_blocked_candidate_has_closure_action",
        ),
        "each_closure_action_has_evidence_ref": _check_passed(
            requirements,
            "each_closure_action_has_evidence_ref",
        ),
        "each_candidate_has_after_state": _check_passed(
            requirements,
            "each_candidate_has_after_state",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
        "remaining_candidate_blocker_closure_records": closure_records_section,
        "remaining_candidate_blocker_before_after_matrix": before_after_matrix,
        "replay_recheck_readiness_handoff": handoff,
        "unresolved_remaining_candidate_blocker_summary": unresolved_summary,
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


def _closure_requirements(
    source_2438g: Mapping[str, Any],
    source_2438f: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    blocked_records: Sequence[Mapping[str, Any]],
    blocker_records: Sequence[Mapping[str, Any]],
    closure_records: Sequence[Mapping[str, Any]],
    remaining_count_before: int,
    remaining_count_after: int,
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    prior_blocked_count = int(source_2438g.get("candidate_replay_blocked_count") or 0)
    closure_record_count_ready = len(closure_records) == 3
    each_has_reason = bool(closure_records) and all(
        bool(record.get("remaining_blocker_reason")) for record in closure_records
    )
    each_has_action = bool(closure_records) and all(
        bool(record.get("closure_action_taken")) for record in closure_records
    )
    each_has_evidence = bool(closure_records) and all(
        bool(record.get("closure_evidence_ref")) for record in closure_records
    )
    each_has_after_state = bool(closure_records) and all(
        bool(record.get("after_state")) for record in closure_records
    )
    records_complete = (
        closure_record_count_ready
        and each_has_reason
        and each_has_action
        and each_has_evidence
        and each_has_after_state
    )
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_closure(research_text)
    )
    return [
        _requirement(
            "prior_2438g_status_is_blocked",
            _source_2438g_ready(source_2438g),
            "prior_2438g_not_blocked",
            "remaining_candidate_replay_blocker_closure_source_gap",
            {
                "status": source_2438g.get("status"),
                "next_route": source_2438g.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "prior_candidate_replay_blocked_count",
            prior_blocked_count == 3 and len(blocked_records) == 3,
            "prior_candidate_replay_blocked_count_not_three",
            "remaining_candidate_replay_blocker_closure_source_gap",
            {
                "prior_candidate_replay_blocked_count": prior_blocked_count,
                "blocked_record_count": len(blocked_records),
            },
        ),
        _requirement(
            "source_2438f_candidate_level_closure_ready",
            _source_2438f_ready(source_2438f),
            "source_2438f_candidate_level_closure_not_ready",
            "candidate_level_closure_source_gap",
            {"status": source_2438f.get("status")},
        ),
        _requirement(
            "candidate_output_records_complete",
            _candidate_outputs_complete(source_2438g, candidate_replay_output_records)
            and len(records) == 3,
            "candidate_output_records_incomplete",
            "candidate_replay_output_record_gap",
            {
                "candidate_output_records_complete": source_2438g.get(
                    "candidate_replay_outputs_complete"
                ),
                "record_count": len(records),
            },
        ),
        _requirement(
            "replayability_handoff_ready",
            _replayability_handoff_ready(source_2438g, source_2438f),
            "replayability_handoff_not_ready",
            "candidate_replayability_handoff_gap",
            {
                "source_2438g_replayability_handoff_ready": source_2438g.get(
                    "replayability_handoff_ready"
                ),
                "source_2438f_replayability_handoff_ready": source_2438f.get(
                    "replayability_handoff_ready"
                ),
            },
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_not_passed",
            "data_quality_gate_gap",
            {"data_quality_status": data_quality_summary.get("data_quality_status")},
        ),
        _requirement(
            "remaining_blocker_records_present",
            bool(blocker_records),
            "remaining_blocker_records_missing",
            "remaining_candidate_blocker_record_gap",
            {"remaining_blocker_record_count": len(blocker_records)},
        ),
        _requirement(
            "remaining_blocker_record_count",
            len(blocker_records) == 3,
            "remaining_blocker_record_count_not_three",
            "remaining_candidate_blocker_record_gap",
            {"remaining_blocker_record_count": len(blocker_records)},
        ),
        _requirement(
            "each_blocked_candidate_has_remaining_blocker_reason",
            each_has_reason,
            "remaining_blocker_reason_missing",
            "remaining_candidate_blocker_record_gap",
            {"missing": _missing_field(closure_records, "remaining_blocker_reason")},
        ),
        _requirement(
            "each_blocked_candidate_has_closure_action",
            each_has_action,
            "closure_action_missing",
            "remaining_candidate_blocker_closure_gap",
            {"missing": _missing_field(closure_records, "closure_action_taken")},
        ),
        _requirement(
            "each_closure_action_has_evidence_ref",
            each_has_evidence,
            "closure_evidence_ref_missing",
            "remaining_candidate_blocker_closure_gap",
            {"missing": _missing_field(closure_records, "closure_evidence_ref")},
        ),
        _requirement(
            "each_candidate_has_after_state",
            each_has_after_state,
            "after_state_missing",
            "remaining_candidate_blocker_closure_gap",
            {"missing": _missing_field(closure_records, "after_state")},
        ),
        _requirement(
            "remaining_candidate_blocker_closure_records_complete",
            records_complete,
            "remaining_candidate_blocker_closure_records_incomplete",
            "remaining_candidate_blocker_closure_gap",
            {"closure_record_count": len(closure_records)},
        ),
        _requirement(
            "remaining_candidate_blocker_count_before",
            remaining_count_before == 3,
            "remaining_candidate_blocker_count_before_not_three",
            "remaining_candidate_blocker_count_gap",
            {"remaining_candidate_blocker_count_before": remaining_count_before},
        ),
        _requirement(
            "remaining_candidate_blocker_count_after",
            remaining_count_after == 0,
            "remaining_candidate_blocker_count_after_nonzero",
            "remaining_candidate_blocker_closure_gap",
            {"remaining_candidate_blocker_count_after": remaining_count_after},
        ),
        _requirement(
            "registry_catalog_docs_alignment",
            registry_catalog_docs_alignment,
            "registry_catalog_docs_not_aligned",
            "registry_catalog_doc_gap",
            {
                "required_report_ids": list(REQUIRED_REPORT_IDS),
                "required_system_flow_references": list(
                    REQUIRED_SYSTEM_FLOW_REFERENCES
                ),
            },
        ),
    ]


def _closure_record(
    candidate_record: Mapping[str, Any],
    blocker_record: Mapping[str, Any] | None,
    source_2438f_record: Mapping[str, Any] | None,
) -> dict[str, Any]:
    blocker = _mapping(blocker_record)
    source_record = _mapping(source_2438f_record)
    candidate_id = str(candidate_record.get("candidate_id") or "")
    categories = _mapped_blocker_categories(blocker, candidate_record)
    primary_category = categories[0] if categories else "other"
    blocker_reason = _optional_string(blocker.get("blocker_reason"))
    source_artifact = _optional_string(
        blocker.get("evidence_ref")
        or candidate_record.get("evidence_ref")
        or blocker.get("closure_evidence_ref")
        or source_record.get("closure_evidence_ref")
    )
    closure_action = _closure_action(blocker, categories, candidate_record)
    evidence_refs = _closure_evidence_refs(blocker, candidate_record, source_record)
    closure_evidence_ref = (
        f"TRADING-2438H:remaining_candidate_blocker_closure:{candidate_id}"
        if evidence_refs and closure_action
        else None
    )
    after_state = (
        {
            "remaining_blocker_after_state": "CLOSED_FOR_2438I_RECHECK",
            "candidate_recheckable_after_closure": True,
            "replay_outcome_after_closure": "NOT_RECHECKED",
            "next_recheck_route": NEXT_ROUTE_READY,
        }
        if blocker_reason and closure_action and closure_evidence_ref
        else None
    )
    closed = bool(blocker_reason and closure_action and closure_evidence_ref and after_state)
    return {
        "candidate_id": candidate_id,
        "prior_replay_status": "BLOCKED",
        "remaining_blocker_category": primary_category,
        "remaining_blocker_categories": categories,
        "prior_blocker_categories": list(_sequence(blocker.get("blocker_category"))),
        "remaining_blocker_reason": blocker_reason,
        "blocker_source_artifact": source_artifact,
        "closure_action_taken": closure_action,
        "closure_evidence_ref": closure_evidence_ref,
        "closure_evidence_refs": evidence_refs,
        "closure_result": "CLOSED" if closed else "STILL_BLOCKED",
        "remaining_blocker_after_closure": None
        if closed
        else _remaining_after_reason(
            blocker_reason,
            closure_action,
            closure_evidence_ref,
            after_state,
        ),
        "candidate_recheckable_after_closure": closed,
        "after_state": after_state,
        "replay_outcome_after_closure": "NOT_RECHECKED",
        "paper_shadow_candidate_found": False,
        "trading_advice_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _closure_action(
    blocker: Mapping[str, Any],
    categories: Sequence[str],
    candidate_record: Mapping[str, Any],
) -> str | None:
    required_next_action = _optional_string(blocker.get("required_next_action"))
    if not required_next_action:
        return None
    candidate_id = candidate_record.get("candidate_id")
    category_text = ", ".join(categories) if categories else "other"
    return (
        "Materialized TRADING-2438H remaining replay blocker closure for "
        f"{candidate_id} by binding 2438G blocker summary, 2438F closure evidence, "
        "candidate replay output record, input spec, source traceability, replay "
        "window, valid-until boundary, outcome linkage and 2438I recheck route; "
        f"prior categories: {category_text}; replay outcome remains NOT_RECHECKED."
    )


def _closure_evidence_refs(
    blocker: Mapping[str, Any],
    candidate_record: Mapping[str, Any],
    source_2438f_record: Mapping[str, Any],
) -> list[str]:
    refs: list[str] = []
    for value in (
        blocker.get("evidence_ref"),
        blocker.get("closure_evidence_ref"),
        candidate_record.get("input_spec_ref"),
        candidate_record.get("source_traceability_ref"),
        candidate_record.get("evidence_ref"),
        candidate_record.get("valid_until_policy_ref"),
        candidate_record.get("outcome_linkage_key"),
        candidate_record.get("forward_aging_handoff_key"),
        source_2438f_record.get("closure_evidence_ref"),
    ):
        _append_ref(refs, value)
    for value in _sequence(source_2438f_record.get("closure_evidence_refs")):
        _append_ref(refs, value)
    return refs


def _mapped_blocker_categories(
    blocker: Mapping[str, Any],
    candidate_record: Mapping[str, Any],
) -> list[str]:
    raw_values = [
        str(value).lower()
        for value in _sequence(blocker.get("blocker_category"))
        + _sequence(candidate_record.get("blocking_gap_ids"))
    ]
    text = " ".join(
        raw_values
        + [
            str(candidate_record.get("source_replay_status", "")).lower(),
            str(_mapping(candidate_record.get("status_reason")).get("blocker_reason", ""))
            .lower(),
        ]
    )
    categories: set[str] = set()
    if "engine" in text or "execution" in text:
        categories.add("unresolved_replay_execution_result")
    if "metric" in text:
        categories.add("missing_candidate_metric_materialization")
    if "baseline" in text:
        categories.add("missing_candidate_baseline_comparison")
    if "pit_window" in text or "as_of" in text or "window" in text:
        categories.add("missing_candidate_replay_window_evidence")
    if "threshold" in text:
        categories.add("missing_candidate_pass_fail_threshold")
    if "failure_reason" in text or "fail_reason" in text:
        categories.add("missing_candidate_failure_reason_rule")
    if "outcome" in text:
        categories.add("missing_candidate_outcome_linkage_materialization")
    if "forward_aging" in text or "handoff" in text:
        categories.add("missing_forward_aging_handoff_materialization")
    if "input" in text or "traceability" in text or "evidence" in text:
        categories.add("unresolved_candidate_evidence_ref")
    if "valid_until" in text or "boundary" in text:
        categories.add("unresolved_candidate_data_boundary")
    if not categories:
        categories.add("other")
    return [
        category
        for category in ALLOWED_REMAINING_BLOCKER_CATEGORIES
        if category in categories
    ]


def _candidate_output_records(
    candidate_replay_output_records: Mapping[str, Any],
    source_2438g: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (candidate_replay_output_records, source_2438g):
        section = _mapping(source.get("candidate_replay_output_records"))
        records = _sequence(section.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("blocked_candidates"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _remaining_blocker_records(
    remaining_candidate_replay_blocker_summary: Mapping[str, Any],
    source_2438g: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (remaining_candidate_replay_blocker_summary, source_2438g):
        section = _mapping(source.get("remaining_candidate_replay_blocker_summary"))
        records = _sequence(section.get("remaining_candidate_replay_blockers"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("remaining_candidate_replay_blockers"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _candidate_level_closure_records(source_2438f: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    section = _mapping(source_2438f.get("candidate_level_blocker_closure_records"))
    records = _sequence(section.get("records"))
    if records:
        return [record for record in records if isinstance(record, Mapping)]
    records = _sequence(source_2438f.get("closed_candidate_blockers"))
    if records:
        return [record for record in records if isinstance(record, Mapping)]
    return []


def _normalize_candidate_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["replay_status"] = str(normalized.get("replay_status", "")).upper()
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


def _records_by_candidate_id(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    return {
        str(record.get("candidate_id")): record
        for record in records
        if record.get("candidate_id")
    }


def _source_2438g_ready(source_2438g: Mapping[str, Any]) -> bool:
    next_route = source_2438g.get("recommended_next_research_task") or source_2438g.get(
        "next_route"
    )
    return source_2438g.get("status") == EXPECTED_2438G_STATUS and (
        next_route == EXPECTED_2438G_ROUTE
    )


def _source_2438f_ready(source_2438f: Mapping[str, Any]) -> bool:
    return (
        source_2438f.get("status") == EXPECTED_2438F_STATUS
        and source_2438f.get("candidate_level_blocker_closure_ready") is True
        and int(source_2438f.get("candidate_level_blocker_count_after") or 0) == 0
    )


def _candidate_outputs_complete(
    source_2438g: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
) -> bool:
    section = _mapping(candidate_replay_output_records.get("candidate_replay_output_records"))
    return (
        source_2438g.get("candidate_replay_outputs_complete") is True
        or section.get("candidate_replay_output_records_ready") is True
    )


def _replayability_handoff_ready(
    source_2438g: Mapping[str, Any],
    source_2438f: Mapping[str, Any],
) -> bool:
    return (
        source_2438g.get("replayability_handoff_ready") is True
        and source_2438f.get("replayability_handoff_ready") is True
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", "")).upper()
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _closure_records_section(
    status: str,
    records: Sequence[Mapping[str, Any]],
    remaining_count_before: int,
    remaining_count_after: int,
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": CLOSURE_RECORDS_SCHEMA_VERSION,
        "status": status,
        "remaining_candidate_blocker_closure_records_ready": (
            remaining_count_after == 0 and len(records) == 3
        ),
        "remaining_candidate_blocker_count_before": remaining_count_before,
        "remaining_candidate_blocker_count_after": remaining_count_after,
        "records": list(records),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _before_after_matrix(
    status: str,
    records: Sequence[Mapping[str, Any]],
    remaining_count_before: int,
    remaining_count_after: int,
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "status": status,
        "before": {
            "remaining_candidate_blocker_count_before": remaining_count_before,
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": len(records),
        },
        "after": {
            "remaining_candidate_blocker_count_after": remaining_count_after,
            "candidate_recheckable_after_closure_count": sum(
                record.get("candidate_recheckable_after_closure") is True
                for record in records
            ),
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": len(records),
            "replay_outcome_after_closure": "NOT_RECHECKED",
        },
        "rows": [
            {
                "candidate_id": record.get("candidate_id"),
                "prior_replay_status": record.get("prior_replay_status"),
                "prior_blocker_category": record.get("remaining_blocker_category"),
                "closure_result": record.get("closure_result"),
                "remaining_blocker_after_closure": record.get(
                    "remaining_blocker_after_closure"
                ),
                "candidate_recheckable_after_closure": record.get(
                    "candidate_recheckable_after_closure"
                ),
                "replay_outcome_after_closure": "NOT_RECHECKED",
                "paper_shadow_candidate_found": False,
                "production_effect": "none",
                "broker_action": "none",
            }
            for record in records
        ],
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _replay_recheck_readiness_handoff(
    status: str,
    closure_ready: bool,
    closure_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    recheckable_candidates = [
        {
            "candidate_id": record.get("candidate_id"),
            "closure_evidence_ref": record.get("closure_evidence_ref"),
            "replay_outcome_after_closure": "NOT_RECHECKED",
            "next_recheck_route": NEXT_ROUTE_READY,
        }
        for record in closure_records
        if record.get("candidate_recheckable_after_closure") is True
    ]
    return {
        "schema_version": REPLAY_RECHECK_HANDOFF_SCHEMA_VERSION,
        "status": status,
        "replay_recheck_handoff_ready": closure_ready,
        "candidate_recheckable_after_closure_count": len(recheckable_candidates),
        "recheckable_candidates": recheckable_candidates,
        "handoff_policy": "RECHECK_ONLY_2438I_DECIDES_PASS_FAIL_BLOCKED",
        "next_route": next_route,
        "candidate_replay_pass_count": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": len(closure_records),
        "forward_aging_handoff_ready": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _unresolved_remaining_blocker_summary(
    status: str,
    unresolved_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": UNRESOLVED_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "unresolved_remaining_candidate_blocker_summary_ready": True,
        "unresolved_remaining_candidate_blocker_count": len(unresolved_blockers),
        "unresolved_remaining_candidate_blockers": list(unresolved_blockers),
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
    remaining_count_after: int,
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "remaining_candidate_blocker_count_after": remaining_count_after,
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
        "forward_aging_handoff_ready": False,
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


def _closed_blocker_summary(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": record.get("candidate_id"),
        "prior_blocker_category": record.get("remaining_blocker_category"),
        "closure_evidence_ref": record.get("closure_evidence_ref"),
        "replay_outcome_after_closure": "NOT_RECHECKED",
    }


def _unresolved_blocker_summary(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": record.get("candidate_id"),
        "remaining_blocker_category": record.get("remaining_blocker_category"),
        "remaining_blocker_reason": record.get("remaining_blocker_after_closure")
        or record.get("remaining_blocker_reason"),
        "closure_result": record.get("closure_result"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_after_reason(
    blocker_reason: str | None,
    closure_action: str | None,
    closure_evidence_ref: str | None,
    after_state: Mapping[str, Any] | None,
) -> str:
    missing: list[str] = []
    if not blocker_reason:
        missing.append("remaining_blocker_reason")
    if not closure_action:
        missing.append("closure_action_taken")
    if not closure_evidence_ref:
        missing.append("closure_evidence_ref")
    if not after_state:
        missing.append("after_state")
    return "Missing remaining blocker closure fields: " + ", ".join(missing)


def _record_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if record.get("candidate_id")
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
    blocker_id: str,
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


def _missing_field(
    records: Sequence[Mapping[str, Any]],
    field: str,
) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if not record.get(field)
    ]


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


def _docs_cover_closure(text: str) -> bool:
    lowered = text.lower()
    return (
        "2438h" in lowered
        and "remaining" in lowered
        and "blocker" in lowered
    )


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return (
            "All three remaining candidate PIT replay blockers are closed for "
            "TRADING-2438I independent recheck; replay outcomes remain NOT_RECHECKED."
        )
    return (
        "At least one remaining candidate PIT replay blocker closure requirement is "
        "still incomplete; continue closure before 2438I can recheck."
    )


def _append_ref(refs: list[str], value: Any) -> None:
    text = _optional_string(value)
    if text and text not in refs:
        refs.append(text)


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
