from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.v1"
CANDIDATE_OUTPUT_RECORDS_SCHEMA_VERSION = (
    "growth_tilt_candidate_replay_output_records.v1"
)
OUTPUT_COMPLETENESS_SCHEMA_VERSION = (
    "growth_tilt_candidate_replay_output_completeness_closure.v1"
)
BEFORE_AFTER_SCHEMA_VERSION = "growth_tilt_candidate_replay_output_before_after.v1"
REMAINING_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_candidate_replay_output_remaining_blockers.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_candidate_replay_output_no_effect.v1"
)

READY_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY"
)
BLOCKED_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_BLOCKED"
)

EXPECTED_2438C_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
EXPECTED_2438C_ROUTE = (
    "TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure"
)
EXPECTED_2438B_STATUS = "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY"
EXPECTED_2438B_ROUTE = (
    "TRADING-2438C_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck"
)
NEXT_ROUTE_READY = (
    "TRADING-2438E_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Output_Closure"
)
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438E_Growth_Tilt_Candidate_Replay_Output_Remaining_Blocker_Closure"
)
REPORT_TYPE = "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure"

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
    "growth_tilt_top3_candidate_pit_replay_recheck",
    "growth_tilt_pit_replay_engine_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/"
    "blocker_closure_result.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/"
    "candidate_replay_output_records.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/"
    "output_completeness_closure.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/"
    "before_after_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/"
    "remaining_output_blocker_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.md",
    "docs/research/growth_tilt_candidate_replay_output_records.md",
    "docs/research/growth_tilt_candidate_replay_output_completeness_closure.md",
    "docs/research/growth_tilt_candidate_replay_output_before_after.md",
    "docs/research/growth_tilt_candidate_replay_output_remaining_blockers.md",
    "docs/research/growth_tilt_candidate_replay_output_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438E_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure",
    READY_STATUS,
    BLOCKED_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_BLOCKED,
)


def build_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure(
    source_2438c_recheck: Mapping[str, Any],
    source_2438b_blocker_closure: Mapping[str, Any],
    source_2438_pit_replay: Mapping[str, Any],
    pit_replay_evidence: Mapping[str, Any],
    pit_replay_blocker_summary: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    candidate_replay_output_records: Sequence[Mapping[str, Any]] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    selected_candidates = _selected_candidates(source_2438c_recheck, source_2438_pit_replay)
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
    resolved_as_of = as_of or str(
        source_2438c_recheck.get("as_of")
        or source_2438b_blocker_closure.get("as_of")
        or source_2438_pit_replay.get("as_of")
        or ""
    )
    records = (
        [dict(record) for record in candidate_replay_output_records]
        if candidate_replay_output_records is not None
        else _default_candidate_output_records(
            selected_candidates,
            selected_ids,
            source_2438c_recheck,
            source_2438b_blocker_closure,
            source_2438_pit_replay,
            evidence_rows,
            resolved_as_of,
        )
    )
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _closure_requirements(
        source_2438c_recheck,
        source_2438b_blocker_closure,
        source_2438_pit_replay,
        blocker_section,
        data_quality_summary,
        selected_ids,
        records,
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
    blocker_closure_ready = not gaps
    status = READY_STATUS if blocker_closure_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if blocker_closure_ready else NEXT_ROUTE_BLOCKED
    normalized_records = [_normalize_record(record) for record in records]
    passing_candidates = _records_with_status(normalized_records, "PASS")
    failed_candidates = _records_with_status(normalized_records, "FAIL")
    blocked_candidates = _records_with_status(normalized_records, "BLOCKED")
    remaining_output_blockers = [
        gap for gap in gaps if gap.get("blocker_id") == "candidate_replay_outputs"
    ]

    output_records_section = _candidate_output_records_section(
        status,
        normalized_records,
        passing_candidates,
        failed_candidates,
        blocked_candidates,
    )
    output_completeness_closure = _output_completeness_closure(
        status,
        requirements,
        blocker_closure_ready,
        remaining_output_blockers,
        next_route,
    )
    before_after_matrix = _before_after_matrix(
        status,
        source_2438c_recheck,
        blocker_closure_ready,
        normalized_records,
        next_route,
        remaining_output_blockers,
    )
    remaining_summary = _remaining_output_blocker_summary(
        status,
        remaining_output_blockers,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438D",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": "TRADING-2438C_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck",
        "prior_status": source_2438c_recheck.get("status"),
        "prior_candidate_replay_outputs_complete": (
            source_2438c_recheck.get("candidate_replay_outputs_complete") is True
        ),
        "source_2438c_recheck_blocked": _source_2438c_blocked_by_output_gap(
            source_2438c_recheck
        ),
        "source_2438b_blocker_closure_ready": _source_2438b_ready(
            source_2438b_blocker_closure
        ),
        "source_2438_pit_replay_artifact_resolves": bool(
            source_2438_pit_replay.get("status")
        ),
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "blocker_closure_ready": blocker_closure_ready,
        "blocker_closed": (
            ["candidate_replay_outputs_complete"] if blocker_closure_ready else []
        ),
        "candidate_replay_outputs_complete": blocker_closure_ready,
        "candidate_replay_output_record_count": len(normalized_records),
        "top3_candidate_ids_present": _check_passed(
            requirements,
            "top3_candidate_ids_present",
        ),
        "each_candidate_has_replay_status": _check_passed(
            requirements,
            "each_candidate_has_replay_status",
        ),
        "each_candidate_has_status_reason": _check_passed(
            requirements,
            "each_candidate_has_status_reason",
        ),
        "each_candidate_has_input_spec_ref": _check_passed(
            requirements,
            "each_candidate_has_input_spec_ref",
        ),
        "each_candidate_has_source_traceability_ref": _check_passed(
            requirements,
            "each_candidate_has_source_traceability_ref",
        ),
        "each_candidate_has_evidence_ref": _check_passed(
            requirements,
            "each_candidate_has_evidence_ref",
        ),
        "each_candidate_has_as_of_boundary": _check_passed(
            requirements,
            "each_candidate_has_as_of_boundary",
        ),
        "each_candidate_has_valid_until_policy_ref": _check_passed(
            requirements,
            "each_candidate_has_valid_until_policy_ref",
        ),
        "each_candidate_has_outcome_linkage_key": _check_passed(
            requirements,
            "each_candidate_has_outcome_linkage_key",
        ),
        "each_candidate_has_forward_aging_handoff_key": _check_passed(
            requirements,
            "each_candidate_has_forward_aging_handoff_key",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
        "selected_candidate_ids": list(selected_ids),
        "top3_candidate_count": len(selected_ids),
        "candidate_replay_pass_count": len(passing_candidates),
        "candidate_replay_fail_count": len(failed_candidates),
        "candidate_replay_blocked_count": len(blocked_candidates),
        "passing_candidates": passing_candidates,
        "failed_candidates": failed_candidates,
        "blocked_candidates": blocked_candidates,
        "candidate_replay_output_records": output_records_section,
        "output_completeness_closure": output_completeness_closure,
        "before_after_matrix": before_after_matrix,
        "remaining_output_blocker_summary": remaining_summary,
        "no_effect_boundary": no_effect_boundary,
        "remaining_output_blockers": remaining_output_blockers,
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


def _closure_requirements(
    source_2438c: Mapping[str, Any],
    source_2438b: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    blocker_section: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    selected_ids: Sequence[str],
    records: Sequence[Mapping[str, Any]],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    record_ids = _record_ids(records)
    top3_ids_present = len(selected_ids) == 3 and len(set(selected_ids)) == 3
    record_count_ready = len(records) == 3 and set(record_ids) == set(selected_ids)
    replay_status_ready = _all_records_have_replay_status(records, selected_ids)
    status_reason_ready = _all_records_have_status_reason(records, selected_ids)
    input_spec_ready = _all_records_have_field(records, selected_ids, "input_spec_ref")
    trace_ref_ready = _all_records_have_field(
        records,
        selected_ids,
        "source_traceability_ref",
    )
    evidence_ref_ready = _all_records_have_field(records, selected_ids, "evidence_ref")
    as_of_ready = _all_records_have_field(records, selected_ids, "as_of")
    valid_until_ready = _all_records_have_field(
        records,
        selected_ids,
        "valid_until_policy_ref",
    )
    outcome_key_ready = _all_records_have_field(
        records,
        selected_ids,
        "outcome_linkage_key",
    )
    handoff_key_ready = _all_records_have_field(
        records,
        selected_ids,
        "forward_aging_handoff_key",
    )
    output_complete = (
        top3_ids_present
        and record_count_ready
        and replay_status_ready
        and status_reason_ready
        and input_spec_ready
        and trace_ref_ready
        and evidence_ref_ready
        and as_of_ready
        and valid_until_ready
        and outcome_key_ready
        and handoff_key_ready
    )
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_closure(research_text)
    )
    return [
        _requirement(
            "source_2438c_recheck_blocked_by_output_gap",
            _source_2438c_blocked_by_output_gap(source_2438c),
            "prior_2438c_recheck_gap",
            None,
            {
                "status": source_2438c.get("status"),
                "next_route": source_2438c.get("recommended_next_research_task"),
                "candidate_replay_outputs_complete": source_2438c.get(
                    "candidate_replay_outputs_complete"
                ),
                "remaining_recheck_blockers": source_2438c.get(
                    "remaining_recheck_blockers"
                ),
            },
        ),
        _requirement(
            "source_2438b_blocker_closure_ready",
            _source_2438b_ready(source_2438b),
            "prior_2438b_closure_gap",
            None,
            {
                "status": source_2438b.get("status"),
                "blocker_closure_ready": source_2438b.get("blocker_closure_ready"),
                "blocker_count_after": source_2438b.get("blocker_count_after"),
            },
        ),
        _requirement(
            "source_2438_pit_replay_artifact_resolves",
            bool(source_2438.get("status")) and bool(blocker_section.get("status")),
            "prior_pit_replay_artifact_gap",
            None,
            {
                "status": source_2438.get("status"),
                "blocker_status": blocker_section.get("status"),
            },
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
            "top3_candidate_ids_present",
            top3_ids_present,
            "top3_candidate_id_gap",
            "candidate_replay_outputs",
            {"selected_candidate_ids": list(selected_ids)},
        ),
        _requirement(
            "candidate_output_record_count",
            record_count_ready,
            "candidate_output_record_count_gap",
            "candidate_replay_outputs",
            {"record_ids": record_ids, "selected_candidate_ids": list(selected_ids)},
        ),
        _requirement(
            "each_candidate_has_replay_status",
            replay_status_ready,
            "candidate_replay_status_gap",
            "candidate_replay_outputs",
            {"missing": _missing_replay_status(records, selected_ids)},
        ),
        _requirement(
            "each_candidate_has_status_reason",
            status_reason_ready,
            "candidate_status_reason_gap",
            "candidate_replay_outputs",
            {"missing": _missing_status_reason(records, selected_ids)},
        ),
        _requirement(
            "each_candidate_has_input_spec_ref",
            input_spec_ready,
            "candidate_input_spec_ref_gap",
            "candidate_replay_outputs",
            {"missing": _missing_field(records, selected_ids, "input_spec_ref")},
        ),
        _requirement(
            "each_candidate_has_source_traceability_ref",
            trace_ref_ready,
            "candidate_source_traceability_ref_gap",
            "candidate_replay_outputs",
            {
                "missing": _missing_field(
                    records,
                    selected_ids,
                    "source_traceability_ref",
                )
            },
        ),
        _requirement(
            "each_candidate_has_evidence_ref",
            evidence_ref_ready,
            "candidate_evidence_ref_gap",
            "candidate_replay_outputs",
            {"missing": _missing_field(records, selected_ids, "evidence_ref")},
        ),
        _requirement(
            "each_candidate_has_as_of_boundary",
            as_of_ready,
            "candidate_as_of_boundary_gap",
            "candidate_replay_outputs",
            {"missing": _missing_field(records, selected_ids, "as_of")},
        ),
        _requirement(
            "each_candidate_has_valid_until_policy_ref",
            valid_until_ready,
            "candidate_valid_until_policy_ref_gap",
            "candidate_replay_outputs",
            {
                "missing": _missing_field(
                    records,
                    selected_ids,
                    "valid_until_policy_ref",
                )
            },
        ),
        _requirement(
            "each_candidate_has_outcome_linkage_key",
            outcome_key_ready,
            "candidate_outcome_linkage_key_gap",
            "candidate_replay_outputs",
            {"missing": _missing_field(records, selected_ids, "outcome_linkage_key")},
        ),
        _requirement(
            "each_candidate_has_forward_aging_handoff_key",
            handoff_key_ready,
            "candidate_forward_aging_handoff_key_gap",
            "candidate_replay_outputs",
            {
                "missing": _missing_field(
                    records,
                    selected_ids,
                    "forward_aging_handoff_key",
                )
            },
        ),
        _requirement(
            "candidate_replay_outputs_complete",
            output_complete,
            "candidate_replay_output_completeness_gap",
            "candidate_replay_outputs",
            {
                "candidate_output_record_count": len(records),
                "selected_candidate_count": len(selected_ids),
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


def _default_candidate_output_records(
    selected_candidates: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
    source_2438c: Mapping[str, Any],
    source_2438b: Mapping[str, Any],
    source_2438: Mapping[str, Any],
    evidence_rows: Sequence[Mapping[str, Any]],
    as_of: str,
) -> list[dict[str, Any]]:
    rows_by_id = {str(row.get("candidate_id")): row for row in evidence_rows}
    result_by_id = _prior_result_by_id(source_2438c)
    candidate_by_id = {
        str(candidate.get("candidate_id")): candidate
        for candidate in selected_candidates
        if candidate.get("candidate_id")
    }
    replay_window = _replay_window(source_2438b)
    baseline_id = _baseline_id(source_2438b)
    outcome_horizons = _outcome_horizons(source_2438b)
    records: list[dict[str, Any]] = []
    for candidate_id in selected_ids:
        candidate = _mapping(candidate_by_id.get(candidate_id))
        row = _mapping(rows_by_id.get(candidate_id))
        prior_result = _mapping(result_by_id.get(candidate_id))
        replay_status = _replay_status_from_prior(prior_result, row)
        blocking_gap_ids = [
            str(gap_id)
            for gap_id in _sequence(
                prior_result.get("blocking_gap_ids") or row.get("blocking_gap_ids")
            )
            if gap_id
        ]
        records.append(
            _normalize_record(
                {
                    "candidate_id": candidate_id,
                    "candidate_family": _candidate_family(candidate, candidate_id),
                    "replay_status": replay_status,
                    "as_of": as_of,
                    "replay_window": replay_window,
                    "baseline_id": baseline_id,
                    "input_spec_ref": _candidate_ref(
                        source_2438b,
                        "input_specs_json",
                        "growth_tilt_pit_replay_input_specs.v1",
                        candidate_id,
                    ),
                    "source_traceability_ref": _candidate_ref(
                        source_2438b,
                        "source_traceability_manifest_json",
                        "growth_tilt_pit_replay_source_traceability.v1",
                        candidate_id,
                    ),
                    "evidence_ref": _candidate_ref(
                        source_2438,
                        "pit_replay_evidence_json",
                        "growth_tilt_top3_candidate_pit_replay_evidence.v1",
                        candidate_id,
                    ),
                    "valid_until_policy_ref": _candidate_ref(
                        source_2438b,
                        "valid_until_boundary_manifest_json",
                        "growth_tilt_pit_replay_valid_until_boundary.v1",
                        candidate_id,
                    ),
                    "outcome_linkage_key": _outcome_linkage_key(
                        candidate_id,
                        outcome_horizons,
                    ),
                    "forward_aging_handoff_key": _forward_aging_handoff_key(
                        candidate_id,
                    ),
                    "metric_summary": _metric_summary(row),
                    "status_reason": _status_reason(
                        replay_status,
                        row,
                        prior_result,
                        blocking_gap_ids,
                    ),
                    "source_replay_status": row.get("pit_replay_status")
                    or prior_result.get("replay_status"),
                    "blocking_gap_ids": blocking_gap_ids,
                    "paper_shadow_candidate_found": False,
                    "trading_advice_generated": False,
                    "broker_order_generated": False,
                    "portfolio_weight_mutated": False,
                    "production_effect": "none",
                    "broker_action": "none",
                }
            )
        )
    return records


def _candidate_output_records_section(
    status: str,
    records: Sequence[Mapping[str, Any]],
    passing_candidates: Sequence[Mapping[str, Any]],
    failed_candidates: Sequence[Mapping[str, Any]],
    blocked_candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_OUTPUT_RECORDS_SCHEMA_VERSION,
        "status": status,
        "candidate_replay_output_records_ready": status == READY_STATUS,
        "candidate_replay_output_record_count": len(records),
        "records": list(records),
        "candidate_replay_pass_count": len(passing_candidates),
        "candidate_replay_fail_count": len(failed_candidates),
        "candidate_replay_blocked_count": len(blocked_candidates),
        "passing_candidates": list(passing_candidates),
        "failed_candidates": list(failed_candidates),
        "blocked_candidates": list(blocked_candidates),
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


def _output_completeness_closure(
    status: str,
    requirements: Sequence[Mapping[str, Any]],
    blocker_closure_ready: bool,
    remaining_output_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    requirement_ids = (
        "top3_candidate_ids_present",
        "candidate_output_record_count",
        "each_candidate_has_replay_status",
        "each_candidate_has_status_reason",
        "each_candidate_has_input_spec_ref",
        "each_candidate_has_source_traceability_ref",
        "each_candidate_has_evidence_ref",
        "each_candidate_has_as_of_boundary",
        "each_candidate_has_valid_until_policy_ref",
        "each_candidate_has_outcome_linkage_key",
        "each_candidate_has_forward_aging_handoff_key",
    )
    return {
        "schema_version": OUTPUT_COMPLETENESS_SCHEMA_VERSION,
        "status": status,
        "blocker_closure_ready": blocker_closure_ready,
        "blocker_closed": (
            ["candidate_replay_outputs_complete"] if blocker_closure_ready else []
        ),
        "candidate_replay_outputs_complete": blocker_closure_ready,
        "requirements": {
            requirement_id: _check_passed(requirements, requirement_id)
            for requirement_id in requirement_ids
        },
        "remaining_output_blockers": list(remaining_output_blockers),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _before_after_matrix(
    status: str,
    source_2438c: Mapping[str, Any],
    blocker_closure_ready: bool,
    records: Sequence[Mapping[str, Any]],
    next_route: str,
    remaining_output_blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "status": status,
        "before": {
            "prior_status": source_2438c.get("status"),
            "prior_next_route": source_2438c.get("recommended_next_research_task"),
            "prior_candidate_replay_outputs_complete": source_2438c.get(
                "candidate_replay_outputs_complete"
            ),
            "prior_candidate_replay_pass_count": source_2438c.get(
                "candidate_replay_pass_count"
            ),
            "prior_candidate_replay_fail_count": source_2438c.get(
                "candidate_replay_fail_count"
            ),
            "prior_candidate_replay_blocked_count": source_2438c.get(
                "candidate_replay_blocked_count"
            ),
        },
        "after": {
            "blocker_closure_ready": blocker_closure_ready,
            "candidate_replay_outputs_complete": blocker_closure_ready,
            "candidate_replay_output_record_count": len(records),
            "candidate_replay_pass_count": len(_records_with_status(records, "PASS")),
            "candidate_replay_fail_count": len(_records_with_status(records, "FAIL")),
            "candidate_replay_blocked_count": len(
                _records_with_status(records, "BLOCKED")
            ),
            "closed_blockers": (
                ["candidate_replay_outputs_complete"] if blocker_closure_ready else []
            ),
            "remaining_output_blockers": list(remaining_output_blockers),
            "next_route": next_route,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_output_blocker_summary(
    status: str,
    remaining_output_blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": REMAINING_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "remaining_output_blocker_summary_ready": True,
        "remaining_output_blocker_count": len(remaining_output_blockers),
        "remaining_output_blockers": list(remaining_output_blockers),
        "next_route": next_route,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_effect_boundary(status: str, gaps: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
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


def _source_2438c_blocked_by_output_gap(payload: Mapping[str, Any]) -> bool:
    remaining_blockers = [
        str(gap.get("blocker_id"))
        for gap in _sequence(payload.get("remaining_recheck_blockers"))
        if isinstance(gap, Mapping)
    ]
    evidence_gap_ids = [str(gap_id) for gap_id in _sequence(payload.get("evidence_gap_ids"))]
    return (
        payload.get("status") == EXPECTED_2438C_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438C_ROUTE
        and payload.get("candidate_replay_outputs_complete") is False
        and (
            "candidate_replay_outputs" in remaining_blockers
            or "candidate_replay_outputs_complete" in evidence_gap_ids
        )
    )


def _source_2438b_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438B_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438B_ROUTE
        and payload.get("blocker_closure_ready") is True
        and _int_or_default(payload.get("blocker_count_after"), -1) == 0
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _selected_candidates(
    source_2438c: Mapping[str, Any],
    source_2438: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    selected = _sequence(source_2438.get("selected_candidates"))
    if not selected:
        selected = _sequence(
            _mapping(source_2438.get("top3_candidate_selection")).get(
                "selected_candidates"
            )
        )
    if selected:
        return [candidate for candidate in selected if isinstance(candidate, Mapping)]
    return [
        {"candidate_id": candidate_id}
        for candidate_id in _sequence(source_2438c.get("selected_candidate_ids"))
        if candidate_id
    ]


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


def _prior_result_by_id(source_2438c: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    results: dict[str, Mapping[str, Any]] = {}
    for key in ("passing_candidates", "failed_candidates", "blocked_candidates"):
        for row in _sequence(source_2438c.get(key)):
            if isinstance(row, Mapping) and row.get("candidate_id"):
                results[str(row["candidate_id"])] = row
    return results


def _replay_status_from_prior(
    prior_result: Mapping[str, Any],
    row: Mapping[str, Any],
) -> str:
    outcome = str(prior_result.get("replay_outcome", "")).lower()
    status = str(row.get("pit_replay_status", prior_result.get("replay_status", ""))).lower()
    if outcome == "pass" or row.get("pit_replay_passed") is True or status in {
        "pass",
        "passed",
    }:
        return "PASS"
    if outcome == "fail" or status in {"fail", "failed", "replay_fail", "no_pass"}:
        return "FAIL"
    return "BLOCKED"


def _candidate_family(candidate: Mapping[str, Any], candidate_id: str) -> str:
    for field in ("candidate_family", "candidate_group", "family"):
        if candidate.get(field):
            return str(candidate[field])
    return candidate_id


def _replay_window(source_2438b: Mapping[str, Any]) -> str:
    replay_window = _mapping(_mapping(source_2438b.get("input_specs")).get("replay_window"))
    return str(replay_window.get("window_id") or "ai_after_chatgpt_pit_replay_window")


def _baseline_id(source_2438b: Mapping[str, Any]) -> str:
    input_specs = _mapping(source_2438b.get("input_specs"))
    return str(input_specs.get("baseline_id") or "growth_tilt_current_policy_baseline")


def _outcome_horizons(source_2438b: Mapping[str, Any]) -> list[str]:
    input_specs = _mapping(source_2438b.get("input_specs"))
    horizons = [str(horizon) for horizon in _sequence(input_specs.get("outcome_horizons"))]
    return horizons or ["1d", "5d", "10d", "20d"]


def _candidate_ref(
    payload: Mapping[str, Any],
    artifact_key: str,
    fallback_schema: str,
    candidate_id: str,
) -> str:
    artifact_path = _mapping(payload.get("artifact_paths")).get(artifact_key)
    if artifact_path:
        return f"{artifact_path}#{candidate_id}"
    return f"{fallback_schema}#{candidate_id}"


def _outcome_linkage_key(candidate_id: str, horizons: Sequence[str]) -> str:
    return f"growth_tilt_pit_replay:{candidate_id}:{','.join(horizons)}"


def _forward_aging_handoff_key(candidate_id: str) -> str:
    return f"TRADING-2439:forward_aging_candidate_pack:{candidate_id}"


def _metric_summary(row: Mapping[str, Any]) -> dict[str, float | None]:
    return {
        key: _float_or_none(row.get(key))
        for key in METRIC_KEYS
    }


def _status_reason(
    replay_status: str,
    row: Mapping[str, Any],
    prior_result: Mapping[str, Any],
    blocking_gap_ids: Sequence[str],
) -> dict[str, str | None]:
    if replay_status == "PASS":
        return {
            "pass_reason": str(
                row.get("pass_reason")
                or prior_result.get("pass_reason")
                or "Candidate replay output is marked PASS by PIT replay evidence."
            ),
            "fail_reason": None,
            "blocker_reason": None,
        }
    if replay_status == "FAIL":
        return {
            "pass_reason": None,
            "fail_reason": str(
                row.get("fail_reason")
                or prior_result.get("fail_reason")
                or "Candidate replay output is marked FAIL by PIT replay evidence."
            ),
            "blocker_reason": None,
        }
    blocker_reason = (
        row.get("blocker_reason")
        or prior_result.get("blocker_reason")
        or f"Candidate remains BLOCKED by: {', '.join(blocking_gap_ids)}"
    )
    return {
        "pass_reason": None,
        "fail_reason": None,
        "blocker_reason": str(blocker_reason),
    }


def _normalize_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["replay_status"] = str(normalized.get("replay_status", "")).upper()
    metric_summary = _mapping(normalized.get("metric_summary"))
    normalized["metric_summary"] = {
        key: _float_or_none(metric_summary.get(key))
        for key in METRIC_KEYS
    }
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


def _record_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(record.get("candidate_id")) for record in records if record.get("candidate_id")]


def _all_records_have_replay_status(
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
) -> bool:
    records_by_id = _records_by_id(records)
    return all(
        str(_mapping(records_by_id.get(candidate_id)).get("replay_status", "")).upper()
        in REPLAY_STATUSES
        for candidate_id in selected_ids
    )


def _all_records_have_status_reason(
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
) -> bool:
    records_by_id = _records_by_id(records)
    for candidate_id in selected_ids:
        record = _mapping(records_by_id.get(candidate_id))
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
    return True


def _all_records_have_field(
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
    field: str,
) -> bool:
    return not _missing_field(records, selected_ids, field)


def _missing_replay_status(
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
) -> list[str]:
    records_by_id = _records_by_id(records)
    return [
        candidate_id
        for candidate_id in selected_ids
        if str(_mapping(records_by_id.get(candidate_id)).get("replay_status", "")).upper()
        not in REPLAY_STATUSES
    ]


def _missing_status_reason(
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
) -> list[str]:
    records_by_id = _records_by_id(records)
    missing: list[str] = []
    for candidate_id in selected_ids:
        record = _mapping(records_by_id.get(candidate_id))
        replay_status = str(record.get("replay_status", "")).upper()
        status_reason = _mapping(record.get("status_reason"))
        if replay_status == "PASS" and not status_reason.get("pass_reason"):
            missing.append(candidate_id)
        elif replay_status == "FAIL" and not status_reason.get("fail_reason"):
            missing.append(candidate_id)
        elif replay_status == "BLOCKED" and not status_reason.get("blocker_reason"):
            missing.append(candidate_id)
        elif replay_status not in REPLAY_STATUSES:
            missing.append(candidate_id)
    return missing


def _missing_field(
    records: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
    field: str,
) -> list[str]:
    records_by_id = _records_by_id(records)
    return [
        candidate_id
        for candidate_id in selected_ids
        if not _mapping(records_by_id.get(candidate_id)).get(field)
    ]


def _records_by_id(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    return {
        str(record.get("candidate_id")): record
        for record in records
        if record.get("candidate_id")
    }


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return (
            "All top-3 candidate replay output records are structurally complete; "
            "route to 2438E for independent pass/fail/blocked recheck."
        )
    return (
        "Candidate replay output records remain incomplete; close remaining "
        "output blockers before the 2438E recheck."
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


def _docs_cover_closure(text: str) -> bool:
    required_terms = ("PIT", "replay", "output", "handoff")
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


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None
