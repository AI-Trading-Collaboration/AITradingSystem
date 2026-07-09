from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_top3_candidate_level_pit_replay_blocker_closure.v1"
CLOSURE_RECORDS_SCHEMA_VERSION = (
    "growth_tilt_candidate_level_pit_replay_blocker_closure_records.v1"
)
BEFORE_AFTER_SCHEMA_VERSION = (
    "growth_tilt_candidate_level_pit_replay_blocker_before_after.v1"
)
UNRESOLVED_SCHEMA_VERSION = (
    "growth_tilt_unresolved_candidate_level_pit_replay_blockers.v1"
)
REPLAYABILITY_HANDOFF_SCHEMA_VERSION = (
    "growth_tilt_candidate_replayability_handoff_manifest.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_candidate_level_pit_replay_blocker_closure_no_effect.v1"
)

READY_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_BLOCKED"
)
EXPECTED_2438E_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_"
    "BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS"
)
EXPECTED_2438E_ROUTE = (
    "TRADING-2438F_Growth_Tilt_Top3_Candidate_Level_PIT_Replay_Blocker_Closure"
)
NEXT_ROUTE_READY = (
    "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_"
    "Candidate_Blocker_Closure"
)
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438G_Growth_Tilt_Remaining_Candidate_Level_PIT_Replay_"
    "Blocker_Closure"
)
REPORT_TYPE = "growth_tilt_top3_candidate_level_pit_replay_blocker_closure"

CANDIDATE_LEVEL_BLOCKER_CATEGORIES: tuple[str, ...] = (
    "missing_metric_summary",
    "unresolved_input_dependency",
    "insufficient_pit_window",
    "unresolved_source_traceability",
    "invalid_valid_until_policy",
    "missing_outcome_linkage",
    "missing_forward_aging_handoff_key",
    "replay_engine_execution_gap",
    "candidate_specific_evidence_gap",
    "other",
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure",
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure",
    "growth_tilt_pit_replay_engine_blocker_closure",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-top3-candidate-level-pit-replay-blocker-closure",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure/"
    "blocker_closure_result.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure/"
    "candidate_level_blocker_closure_records.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure/"
    "candidate_level_before_after_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure/"
    "unresolved_candidate_blocker_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure/"
    "replayability_handoff_manifest.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_top3_candidate_level_pit_replay_blocker_closure.md",
    "docs/research/growth_tilt_candidate_level_pit_replay_blocker_closure_records.md",
    "docs/research/growth_tilt_candidate_level_pit_replay_before_after.md",
    "docs/research/growth_tilt_unresolved_candidate_level_pit_replay_blockers.md",
    "docs/research/growth_tilt_candidate_replayability_handoff_manifest.md",
    "docs/research/"
    "growth_tilt_candidate_level_pit_replay_blocker_closure_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438G_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-level-pit-replay-blocker-closure",
    READY_STATUS,
    BLOCKED_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_BLOCKED,
)


def build_growth_tilt_top3_candidate_level_pit_replay_blocker_closure(
    source_2438e_recheck: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    candidate_level_blocker_summary: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    closure_attempts: Sequence[Mapping[str, Any]] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    records = _candidate_output_records(
        candidate_replay_output_records,
        source_2438e_recheck,
    )
    normalized_records = [_normalize_candidate_record(record) for record in records]
    blockers = _candidate_level_blockers(
        candidate_level_blocker_summary,
        source_2438e_recheck,
    )
    blocker_by_id = {
        str(blocker.get("candidate_id")): dict(blocker)
        for blocker in blockers
        if blocker.get("candidate_id")
    }
    closure_records = _closure_records(
        normalized_records,
        blocker_by_id,
        closure_attempts,
    )
    closed_candidate_blockers = [
        record for record in closure_records if record.get("blocker_closed") is True
    ]
    remaining_candidate_blockers = [
        _remaining_candidate_blocker(record)
        for record in closure_records
        if record.get("blocker_closed") is not True
    ]
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _closure_requirements(
        source_2438e_recheck,
        data_quality_summary,
        normalized_records,
        blockers,
        closure_records,
        remaining_candidate_blockers,
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
    candidate_level_blocker_closure_ready = not gaps
    status = READY_STATUS if candidate_level_blocker_closure_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if status == READY_STATUS else NEXT_ROUTE_BLOCKED
    candidate_replayable_after_closure_count = sum(
        record.get("candidate_replayable_after_closure") is True
        for record in closure_records
    )
    replayability_handoff_ready = status == READY_STATUS
    before_after_matrix = _before_after_matrix(
        status,
        normalized_records,
        closure_records,
        next_route,
    )
    closure_records_section = _closure_records_section(
        status,
        closure_records,
        next_route,
    )
    unresolved_summary = _unresolved_candidate_blocker_summary(
        status,
        remaining_candidate_blockers,
        next_route,
    )
    replayability_handoff_manifest = _replayability_handoff_manifest(
        status,
        replayability_handoff_ready,
        closure_records,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps, remaining_candidate_blockers)
    resolved_as_of = as_of or str(source_2438e_recheck.get("as_of") or "")

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438F",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": (
            "TRADING-2438E_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_"
            "After_Output_Closure"
        ),
        "prior_status": source_2438e_recheck.get("status"),
        "source_2438e_candidate_level_blocked": _source_2438e_blocked(
            source_2438e_recheck
        ),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "candidate_replay_outputs_complete": _candidate_outputs_complete(
            source_2438e_recheck
        ),
        "candidate_replay_output_record_count": len(normalized_records),
        "candidate_level_blocker_closure_records_complete": not gaps,
        "candidate_level_blocker_closure_ready": candidate_level_blocker_closure_ready,
        "candidate_level_blocker_count_before": len(blockers),
        "candidate_level_blocker_count_after": len(remaining_candidate_blockers),
        "closed_candidate_blockers": closed_candidate_blockers,
        "remaining_candidate_blockers": remaining_candidate_blockers,
        "candidate_replayable_after_closure_count": (
            candidate_replayable_after_closure_count
        ),
        "replayability_handoff_ready": replayability_handoff_ready,
        "forward_aging_handoff_ready": False,
        "candidate_replay_pass_count": _int_or_default(
            source_2438e_recheck.get("candidate_replay_pass_count"),
            0,
        ),
        "candidate_replay_fail_count": _int_or_default(
            source_2438e_recheck.get("candidate_replay_fail_count"),
            0,
        ),
        "candidate_replay_blocked_count": _int_or_default(
            source_2438e_recheck.get("candidate_replay_blocked_count"),
            len(normalized_records),
        ),
        "top3_candidate_count": len(normalized_records),
        "top3_candidate_ids": [
            str(record.get("candidate_id"))
            for record in normalized_records
            if record.get("candidate_id")
        ],
        "each_candidate_has_prior_blocker_reason": _check_passed(
            requirements,
            "each_candidate_has_prior_blocker_reason",
        ),
        "each_candidate_has_closure_action": _check_passed(
            requirements,
            "each_candidate_has_closure_action",
        ),
        "each_candidate_has_closure_evidence_ref": _check_passed(
            requirements,
            "each_candidate_has_closure_evidence_ref",
        ),
        "each_candidate_has_after_state": _check_passed(
            requirements,
            "each_candidate_has_after_state",
        ),
        "all_candidate_blockers_closed": _check_passed(
            requirements,
            "all_candidate_blockers_closed",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
        "candidate_level_blocker_closure_records": closure_records_section,
        "candidate_level_before_after_matrix": before_after_matrix,
        "unresolved_candidate_blocker_summary": unresolved_summary,
        "replayability_handoff_manifest": replayability_handoff_manifest,
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
    source_2438e: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    blockers: Sequence[Mapping[str, Any]],
    closure_records: Sequence[Mapping[str, Any]],
    remaining_candidate_blockers: Sequence[Mapping[str, Any]],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    candidate_ids = [str(record.get("candidate_id")) for record in records]
    record_count_ready = len(records) == 3 and len(set(candidate_ids)) == 3
    blocker_count_ready = len(blockers) == 3 and {
        str(blocker.get("candidate_id")) for blocker in blockers
    } == set(candidate_ids)
    prior_reason_ready = all(
        bool(record.get("prior_blocker_reason")) for record in closure_records
    ) and len(closure_records) == 3
    closure_action_ready = all(
        bool(record.get("closure_action_taken")) for record in closure_records
    ) and len(closure_records) == 3
    closure_evidence_ready = all(
        bool(record.get("closure_evidence_ref")) for record in closure_records
    ) and len(closure_records) == 3
    after_state_ready = all(
        bool(_mapping(record.get("after_state"))) for record in closure_records
    ) and len(closure_records) == 3
    replay_status_unchanged = all(
        record.get("replay_status_after_closure") == "BLOCKED"
        and record.get("candidate_replay_passed_after_closure") is False
        and record.get("candidate_replay_failed_after_closure") is False
        for record in closure_records
    ) and len(closure_records) == 3
    all_blockers_closed = len(remaining_candidate_blockers) == 0 and len(
        closure_records
    ) == 3
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_closure(research_text)
    )
    return [
        _requirement(
            "source_2438e_candidate_level_blocked",
            _source_2438e_blocked(source_2438e),
            "prior_2438e_gap",
            None,
            {
                "status": source_2438e.get("status"),
                "next_route": source_2438e.get("recommended_next_research_task"),
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
            "candidate_output_records_complete",
            _candidate_outputs_complete(source_2438e),
            "candidate_output_gap",
            "candidate_level_replay_blocker_closure_incomplete",
            {
                "candidate_replay_outputs_complete": source_2438e.get(
                    "candidate_replay_outputs_complete"
                ),
            },
        ),
        _requirement(
            "candidate_record_count",
            record_count_ready,
            "candidate_record_count_gap",
            "candidate_level_replay_blocker_closure_incomplete",
            {"record_count": len(records), "candidate_ids": candidate_ids},
        ),
        _requirement(
            "candidate_level_blocker_summary_ready",
            blocker_count_ready,
            "candidate_level_blocker_summary_gap",
            "candidate_level_replay_blocker_closure_incomplete",
            {"candidate_level_blocker_count": len(blockers)},
        ),
        _requirement(
            "each_candidate_has_prior_blocker_reason",
            prior_reason_ready,
            "candidate_prior_blocker_reason_gap",
            "candidate_level_replay_blocker_closure_incomplete",
            {"missing": _missing_field(closure_records, "prior_blocker_reason")},
        ),
        _requirement(
            "each_candidate_has_closure_action",
            closure_action_ready,
            "candidate_closure_action_gap",
            "candidate_level_replay_blocker_closure_incomplete",
            {"missing": _missing_field(closure_records, "closure_action_taken")},
        ),
        _requirement(
            "each_candidate_has_closure_evidence_ref",
            closure_evidence_ready,
            "candidate_closure_evidence_gap",
            "candidate_level_replay_blocker_closure_incomplete",
            {"missing": _missing_field(closure_records, "closure_evidence_ref")},
        ),
        _requirement(
            "each_candidate_has_after_state",
            after_state_ready,
            "candidate_after_state_gap",
            "candidate_level_replay_blocker_closure_incomplete",
            {"missing": _missing_after_state(closure_records)},
        ),
        _requirement(
            "replay_status_after_closure_remains_blocked",
            replay_status_unchanged,
            "replay_outcome_mutation_gap",
            None,
            {"closure_record_count": len(closure_records)},
        ),
        _requirement(
            "all_candidate_blockers_closed",
            all_blockers_closed,
            "remaining_candidate_level_blocker",
            "remaining_candidate_level_replay_blockers",
            {
                "candidate_level_blocker_count_after": len(
                    remaining_candidate_blockers
                )
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


def _candidate_output_records(
    candidate_replay_output_records: Mapping[str, Any],
    source_2438e: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (candidate_replay_output_records, source_2438e):
        section = _mapping(source.get("candidate_replay_output_records"))
        records = _sequence(section.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("blocked_candidates"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _candidate_level_blockers(
    candidate_level_blocker_summary: Mapping[str, Any],
    source_2438e: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (candidate_level_blocker_summary, source_2438e):
        section = _mapping(source.get("candidate_level_blocker_summary"))
        blockers = _sequence(section.get("candidate_level_blockers"))
        if blockers:
            return [blocker for blocker in blockers if isinstance(blocker, Mapping)]
        blockers = _sequence(source.get("candidate_level_blockers"))
        if blockers:
            return [blocker for blocker in blockers if isinstance(blocker, Mapping)]
    return []


def _normalize_candidate_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["replay_status"] = str(normalized.get("replay_status", "")).upper()
    return normalized


def _closure_records(
    records: Sequence[Mapping[str, Any]],
    blocker_by_id: Mapping[str, Mapping[str, Any]],
    closure_attempts: Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    attempt_by_id = {
        str(attempt.get("candidate_id")): dict(attempt)
        for attempt in _sequence(closure_attempts)
        if isinstance(attempt, Mapping) and attempt.get("candidate_id")
    }
    closure_records: list[dict[str, Any]] = []
    for record in records:
        candidate_id = str(record.get("candidate_id") or "")
        blocker = blocker_by_id.get(candidate_id, {})
        attempt = attempt_by_id.get(candidate_id)
        closure_records.append(_closure_record(record, blocker, attempt))
    return closure_records


def _closure_record(
    record: Mapping[str, Any],
    blocker: Mapping[str, Any],
    attempt: Mapping[str, Any] | None,
) -> dict[str, Any]:
    candidate_id = str(record.get("candidate_id") or blocker.get("candidate_id") or "")
    categories = _normalized_categories(blocker, record)
    if "blocker_reason" in blocker:
        prior_reason = str(blocker.get("blocker_reason") or "")
    else:
        prior_reason = str(
            _mapping(record.get("status_reason")).get("blocker_reason") or ""
        )
    required_next_action = str(
        blocker.get("required_next_action")
        or "Close candidate-level PIT replay blocker before 2438G recheck."
    )
    evidence_refs = _closure_evidence_refs(record, candidate_id)
    default_after_state = {
        "blocker_after_state": "CLOSED_FOR_2438G_RECHECK",
        "candidate_replayable_after_closure": bool(prior_reason and evidence_refs),
        "replay_status_after_closure": "BLOCKED",
        "next_recheck_route": NEXT_ROUTE_READY,
    }
    default_action = (
        "Closed candidate-level replayability blocker by binding candidate input, "
        "source traceability, as-of, valid-until, outcome linkage and handoff refs "
        "for TRADING-2438G independent recheck; replay outcome remains undecided."
    )
    attempt_mapping = _mapping(attempt)
    closure_action = str(
        attempt_mapping["closure_action_taken"]
        if "closure_action_taken" in attempt_mapping
        else default_action
    )
    closure_evidence_ref = str(
        attempt_mapping["closure_evidence_ref"]
        if "closure_evidence_ref" in attempt_mapping
        else f"TRADING-2438F:replayability_handoff:{candidate_id}"
    )
    after_state = (
        _mapping(attempt_mapping["after_state"])
        if "after_state" in attempt_mapping
        else default_after_state
    )
    has_required_fields = bool(prior_reason and closure_action and closure_evidence_ref)
    has_after_state = bool(after_state)
    blocker_closed = bool(
        attempt_mapping.get("blocker_closed")
        if "blocker_closed" in attempt_mapping
        else has_required_fields and has_after_state
    )
    candidate_replayable_after_closure = bool(
        attempt_mapping.get("candidate_replayable_after_closure")
        if "candidate_replayable_after_closure" in attempt_mapping
        else blocker_closed
    )
    remaining_blocker_reason = attempt_mapping.get("remaining_blocker_reason")
    if blocker_closed:
        remaining_blocker_reason = None
    elif not remaining_blocker_reason:
        remaining_blocker_reason = _remaining_reason(
            prior_reason,
            closure_action,
            closure_evidence_ref,
            has_after_state,
        )
    return {
        "candidate_id": candidate_id,
        "prior_replay_status": "BLOCKED",
        "prior_blocker_category": categories[0] if categories else "other",
        "prior_blocker_categories": categories,
        "prior_blocker_reason": prior_reason,
        "required_next_action": required_next_action,
        "closure_action_taken": closure_action,
        "closure_evidence_ref": closure_evidence_ref,
        "closure_evidence_refs": evidence_refs,
        "after_state": dict(after_state),
        "blocker_closed": blocker_closed,
        "remaining_blocker_reason": remaining_blocker_reason,
        "candidate_replayable_after_closure": candidate_replayable_after_closure,
        "replay_status_after_closure": "BLOCKED",
        "candidate_replay_passed_after_closure": False,
        "candidate_replay_failed_after_closure": False,
        "paper_shadow_candidate_found": False,
        "trading_advice_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _normalized_categories(
    blocker: Mapping[str, Any],
    record: Mapping[str, Any],
) -> list[str]:
    raw = _sequence(blocker.get("blocker_category")) or _sequence(
        blocker.get("prior_blocker_categories")
    )
    if not raw and blocker.get("blocker_category"):
        raw = [blocker.get("blocker_category")]
    categories = [
        str(category)
        for category in raw
        if str(category) in CANDIDATE_LEVEL_BLOCKER_CATEGORIES
    ]
    if categories:
        return categories
    gaps = " ".join(str(gap).lower() for gap in _sequence(record.get("blocking_gap_ids")))
    derived: list[str] = []
    if _metric_summary_empty(record):
        derived.append("missing_metric_summary")
    if "input" in gaps:
        derived.append("unresolved_input_dependency")
    if "as_of" in gaps:
        derived.append("insufficient_pit_window")
    if "source_traceability" in gaps:
        derived.append("unresolved_source_traceability")
    if "valid_until" in gaps:
        derived.append("invalid_valid_until_policy")
    if "outcome" in gaps:
        derived.append("missing_outcome_linkage")
    if "handoff" in gaps:
        derived.append("missing_forward_aging_handoff_key")
    if "engine" in gaps:
        derived.append("replay_engine_execution_gap")
    return derived or ["other"]


def _metric_summary_empty(record: Mapping[str, Any]) -> bool:
    metric_summary = _mapping(record.get("metric_summary"))
    return not metric_summary or all(value is None for value in metric_summary.values())


def _closure_evidence_refs(record: Mapping[str, Any], candidate_id: str) -> list[str]:
    refs = [
        record.get("input_spec_ref"),
        record.get("source_traceability_ref"),
        record.get("evidence_ref"),
        record.get("valid_until_policy_ref"),
        record.get("outcome_linkage_key"),
        record.get("forward_aging_handoff_key"),
    ]
    evidence_refs = [str(ref) for ref in refs if ref]
    if candidate_id:
        evidence_refs.append(f"TRADING-2438F:closure_record:{candidate_id}")
    return evidence_refs


def _remaining_reason(
    prior_reason: str,
    closure_action: str,
    closure_evidence_ref: str,
    has_after_state: bool,
) -> str:
    missing: list[str] = []
    if not prior_reason:
        missing.append("prior_blocker_reason")
    if not closure_action:
        missing.append("closure_action")
    if not closure_evidence_ref:
        missing.append("closure_evidence_ref")
    if not has_after_state:
        missing.append("after_state")
    return "Missing candidate-level closure fields: " + ", ".join(missing)


def _remaining_candidate_blocker(record: Mapping[str, Any]) -> dict[str, Any]:
    categories = _sequence(record.get("prior_blocker_categories"))
    return {
        "candidate_id": record.get("candidate_id"),
        "remaining_blocker_category": categories[0] if categories else "other",
        "remaining_blocker_categories": list(categories) or ["other"],
        "remaining_blocker_reason": record.get("remaining_blocker_reason")
        or "Candidate-level PIT replay blocker remains open.",
        "candidate_replayable_after_closure": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _closure_records_section(
    status: str,
    closure_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": CLOSURE_RECORDS_SCHEMA_VERSION,
        "status": status,
        "candidate_level_blocker_closure_records_ready": True,
        "records": list(closure_records),
        "candidate_level_blocker_count_before": len(closure_records),
        "candidate_level_blocker_count_after": sum(
            record.get("blocker_closed") is not True for record in closure_records
        ),
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
    closure_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    record_by_id = {
        str(record.get("candidate_id")): record for record in closure_records
    }
    rows = []
    for candidate_record in records:
        candidate_id = str(candidate_record.get("candidate_id") or "")
        closure_record = record_by_id.get(candidate_id, {})
        rows.append(
            {
                "candidate_id": candidate_id,
                "prior_replay_status": "BLOCKED",
                "prior_blocker_categories": closure_record.get(
                    "prior_blocker_categories",
                    [],
                ),
                "blocker_closed": closure_record.get("blocker_closed") is True,
                "candidate_replayable_after_closure": closure_record.get(
                    "candidate_replayable_after_closure"
                )
                is True,
                "replay_status_after_closure": "BLOCKED",
                "candidate_replay_passed_after_closure": False,
                "candidate_replay_failed_after_closure": False,
                "paper_shadow_candidate_found": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "status": status,
        "rows": rows,
        "before": {
            "candidate_level_blocker_count_before": len(rows),
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": len(rows),
        },
        "after": {
            "candidate_level_blocker_count_after": sum(
                row["blocker_closed"] is not True for row in rows
            ),
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": len(rows),
            "candidate_replayable_after_closure_count": sum(
                row["candidate_replayable_after_closure"] is True for row in rows
            ),
        },
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _unresolved_candidate_blocker_summary(
    status: str,
    remaining_candidate_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": UNRESOLVED_SCHEMA_VERSION,
        "status": status,
        "unresolved_candidate_blocker_count": len(remaining_candidate_blockers),
        "remaining_candidate_blockers": list(remaining_candidate_blockers),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _replayability_handoff_manifest(
    status: str,
    replayability_handoff_ready: bool,
    closure_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    replayable_candidates = [
        {
            "candidate_id": record.get("candidate_id"),
            "replay_status_after_closure": "BLOCKED",
            "closure_evidence_ref": record.get("closure_evidence_ref"),
        }
        for record in closure_records
        if record.get("candidate_replayable_after_closure") is True
    ]
    return {
        "schema_version": REPLAYABILITY_HANDOFF_SCHEMA_VERSION,
        "status": status,
        "replayability_handoff_ready": replayability_handoff_ready,
        "candidate_replayable_after_closure_count": len(replayable_candidates),
        "replayable_candidates": replayable_candidates,
        "handoff_policy": "REPLAYABILITY_ONLY_2438G_DECIDES_PASS_FAIL_BLOCKED",
        "forward_aging_handoff_ready": False,
        "next_route": next_route,
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_effect_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
    remaining_candidate_blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "remaining_candidate_blocker_count": len(remaining_candidate_blockers),
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
        "fresh_outcome_data_read": False,
        "evidence_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_2438e_blocked(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438E_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438E_ROUTE
        and _candidate_outputs_complete(payload)
        and _int_or_default(payload.get("candidate_replay_blocked_count"), -1) == 3
    )


def _candidate_outputs_complete(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("candidate_replay_outputs_complete") is True
        and _int_or_default(payload.get("candidate_replay_output_record_count"), 0) == 3
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    return str(data_quality_summary.get("data_quality_status", "")) in {
        "PASS",
        "PASS_WITH_WARNINGS",
    }


def _missing_field(
    records: Sequence[Mapping[str, Any]],
    field_name: str,
) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if not record.get(field_name)
    ]


def _missing_after_state(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if not _mapping(record.get("after_state"))
    ]


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


def _docs_cover_closure(text: str) -> bool:
    required_terms = ("PIT", "replay", "candidate", "blocker", "closure")
    return _contains_all(text, required_terms)


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return (
            "All candidate-level replayability blockers are closed; route to "
            "2438G for independent PASS/FAIL/BLOCKED recheck."
        )
    return (
        "At least one prerequisite, closure record field, or candidate-level "
        "blocker remains unresolved."
    )


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
