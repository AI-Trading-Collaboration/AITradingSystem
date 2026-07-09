from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_persistent_candidate_pit_replay_blocker_escalation.v1"
ROOT_CAUSE_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_candidate_persistent_blocker_root_cause_matrix.v1"
)
REPEATED_CLOSURE_SCHEMA_VERSION = "growth_tilt_repeated_closure_failure_summary.v1"
REMEDIATION_ROUTE_SCHEMA_VERSION = (
    "growth_tilt_persistent_blocker_recommended_remediation_route.v1"
)
NO_FORWARD_AGING_SCHEMA_VERSION = "growth_tilt_no_forward_aging_safety_decision.v1"

READY_STATUS = "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ESCALATION_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ESCALATION_BLOCKED"
)

EXPECTED_2438I_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_"
    "BLOCKER_CLOSURE_BLOCKED"
)
EXPECTED_2438I_ROUTE = (
    "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_"
    "Escalation"
)
EXPECTED_2438H_STATUS = "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY"
EXPECTED_2438F_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY"
)
EXPECTED_2438D_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY"
EXPECTED_2438B_STATUS = "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY"
NEXT_ROUTE_ROOT_CAUSE_REMEDIATION = (
    "TRADING-2438K_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_"
    "Root_Cause_Remediation"
)
NEXT_ROUTE_DEFINITION_REBUILD = (
    "TRADING-2438K_Growth_Tilt_Top3_Candidate_Definition_Rebuild_Or_"
    "Reselection"
)
NEXT_ROUTE_MANUAL_REVIEW = (
    "TRADING-2438K_Growth_Tilt_Persistent_PIT_Replay_Blocker_Manual_Review"
)
REPORT_TYPE = "growth_tilt_persistent_candidate_pit_replay_blocker_escalation"

ROOT_CAUSE_CATEGORIES: tuple[str, ...] = (
    "replay_execution_not_materialized",
    "candidate_metric_materialization_missing",
    "baseline_comparison_not_materialized",
    "pass_fail_threshold_not_executable",
    "candidate_evidence_chain_incomplete_despite_closure",
    "candidate_replay_window_unresolvable",
    "candidate_input_spec_semantically_incomplete",
    "outcome_linkage_not_materialized",
    "forward_aging_handoff_not_materialized",
    "candidate_definition_not_replayable",
    "top3_selection_artifact_not_sufficient_for_replay",
    "replay_engine_contract_ready_but_runtime_not_executable",
    "other",
)
ROOT_CAUSE_ROUTE_DEFINITION_REBUILD: tuple[str, ...] = (
    "candidate_definition_not_replayable",
    "top3_selection_artifact_not_sufficient_for_replay",
)
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
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure",
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure",
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure",
    "growth_tilt_pit_replay_engine_blocker_closure",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-persistent-candidate-pit-replay-blocker-escalation",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_escalation/"
    "escalation_result.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_escalation/"
    "candidate_persistent_blocker_root_cause_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_escalation/"
    "repeated_closure_failure_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_escalation/"
    "recommended_remediation_route.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_escalation/"
    "no_forward_aging_safety_decision.json",
    "docs/research/growth_tilt_persistent_candidate_pit_replay_blocker_escalation.md",
    "docs/research/growth_tilt_candidate_persistent_blocker_root_cause_matrix.md",
    "docs/research/growth_tilt_repeated_closure_failure_summary.md",
    "docs/research/growth_tilt_persistent_blocker_recommended_remediation_route.md",
    "docs/research/growth_tilt_no_forward_aging_safety_decision.md",
    "docs/research/dynamic_strategy_2438K_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-persistent-candidate-pit-replay-blocker-escalation",
    READY_STATUS,
    BLOCKED_STATUS,
    EXPECTED_2438I_STATUS,
    NEXT_ROUTE_ROOT_CAUSE_REMEDIATION,
    NEXT_ROUTE_DEFINITION_REBUILD,
    NEXT_ROUTE_MANUAL_REVIEW,
    "NOT_RECHECKED",
)


def build_growth_tilt_persistent_candidate_pit_replay_blocker_escalation(
    source_2438i_blocked_recheck: Mapping[str, Any],
    persistent_candidate_replay_blocker_summary: Mapping[str, Any],
    source_2438h_remaining_blocker_closure: Mapping[str, Any],
    source_2438f_candidate_level_blocker_closure: Mapping[str, Any],
    source_2438d_output_closure: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    source_2438b_engine_blocker_closure: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    candidate_records = _candidate_records(
        candidate_replay_output_records,
        source_2438i_blocked_recheck,
        source_2438d_output_closure,
    )
    normalized_records = [_normalize_candidate_record(record) for record in candidate_records]
    persistent_blockers = _persistent_blockers(
        persistent_candidate_replay_blocker_summary,
        source_2438i_blocked_recheck,
    )
    closure_history = _closure_history(
        source_2438b_engine_blocker_closure,
        source_2438d_output_closure,
        source_2438f_candidate_level_blocker_closure,
        source_2438h_remaining_blocker_closure,
    )
    escalation_records = _escalation_records(
        normalized_records,
        persistent_blockers,
        closure_history,
    )
    root_cause_rows = _root_cause_rows(escalation_records)
    next_route = _recommended_next_route(root_cause_rows)
    pass_count = _replay_status_count(normalized_records, "PASS")
    fail_count = _replay_status_count(normalized_records, "FAIL")
    blocked_count = _replay_status_count(normalized_records, "BLOCKED")
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _escalation_requirements(
        source_2438i_blocked_recheck,
        source_2438h_remaining_blocker_closure,
        source_2438f_candidate_level_blocker_closure,
        source_2438d_output_closure,
        source_2438b_engine_blocker_closure,
        data_quality_summary,
        normalized_records,
        persistent_blockers,
        escalation_records,
        closure_history,
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
    escalation_ready = not gaps
    status = READY_STATUS if escalation_ready else BLOCKED_STATUS
    if not escalation_ready:
        next_route = NEXT_ROUTE_MANUAL_REVIEW
    root_cause_matrix = _root_cause_matrix(status, root_cause_rows, next_route)
    repeated_summary = _repeated_closure_failure_summary(
        status,
        closure_history,
        escalation_records,
        next_route,
    )
    remediation_route = _recommended_remediation_route(status, root_cause_rows, next_route)
    no_forward_aging = _no_forward_aging_safety_decision(
        status,
        escalation_records,
        next_route,
    )
    resolved_as_of = as_of or str(
        source_2438i_blocked_recheck.get("as_of")
        or _first_record_as_of(normalized_records)
        or ""
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438J",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": (
            "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_"
            "After_Remaining_Blocker_Closure"
        ),
        "prior_status": source_2438i_blocked_recheck.get("status"),
        "source_2438i_blocked_recheck_ready": _source_2438i_ready(
            source_2438i_blocked_recheck
        ),
        "persistent_blocker_escalation_required": _persistent_escalation_required(
            source_2438i_blocked_recheck
        ),
        "persistent_blocker_escalation_ready": escalation_ready,
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "candidate_replay_pass_count": pass_count,
        "candidate_replay_fail_count": fail_count,
        "candidate_replay_blocked_count": blocked_count,
        "persistent_blocked_candidate_count": len(escalation_records),
        "persistent_candidate_replay_blocker_count": len(persistent_blockers),
        "candidate_replay_output_record_count": len(normalized_records),
        "candidate_replay_outputs_complete": _candidate_outputs_complete(
            source_2438d_output_closure,
            candidate_replay_output_records,
            source_2438i_blocked_recheck,
        ),
        "closure_history_confirmed": _closure_history_confirmed(closure_history),
        "closure_history": closure_history,
        "pit_replay_engine_blocker_closure_ready": closure_history[
            "pit_replay_engine_blocker_closure_ready"
        ],
        "output_completeness_closure_ready": closure_history[
            "output_completeness_closure_ready"
        ],
        "candidate_level_blocker_closure_ready": closure_history[
            "candidate_level_blocker_closure_ready"
        ],
        "remaining_blocker_closure_ready": closure_history[
            "remaining_blocker_closure_ready"
        ],
        "candidate_persistent_blocker_escalation_records": escalation_records,
        "persistent_blocker_root_causes": root_cause_rows,
        "candidate_persistent_blocker_root_cause_matrix": root_cause_matrix,
        "repeated_closure_failure_summary": repeated_summary,
        "recommended_remediation_route": remediation_route,
        "no_forward_aging_safety_decision": no_forward_aging,
        "forward_aging_handoff_ready": False,
        "forward_aging_candidate_count": 0,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "all_escalation_records_have_root_cause_category": _check_passed(
            requirements,
            "all_escalation_records_have_root_cause_category",
        ),
        "all_escalation_records_have_root_cause_layer": _check_passed(
            requirements,
            "all_escalation_records_have_root_cause_layer",
        ),
        "all_escalation_records_have_recommended_next_action": _check_passed(
            requirements,
            "all_escalation_records_have_recommended_next_action",
        ),
        "all_escalation_records_have_blocker_reason": _check_passed(
            requirements,
            "all_escalation_records_have_blocker_reason",
        ),
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
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
        "paper_shadow_allowed": False,
        "paper_shadow_approved": False,
        "paper_shadow_daily_job_run": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "production_allowed": False,
        "broker_action_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "generated_signal": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "daily_report_generated": False,
        "actionable_allocation_generated": False,
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": _next_route_reason(status, next_route),
    }


def _escalation_requirements(
    source_2438i: Mapping[str, Any],
    source_2438h: Mapping[str, Any],
    source_2438f: Mapping[str, Any],
    source_2438d: Mapping[str, Any],
    source_2438b: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    persistent_blockers: Sequence[Mapping[str, Any]],
    escalation_records: Sequence[Mapping[str, Any]],
    closure_history: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    record_ids = _candidate_ids(records)
    blocker_ids = _candidate_ids(persistent_blockers)
    escalation_ids = _candidate_ids(escalation_records)
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_escalation(research_text)
    )
    return [
        _requirement(
            "source_2438i_blocked_recheck_ready",
            _source_2438i_ready(source_2438i),
            "source_2438i_not_blocked_for_2438j",
            "persistent_escalation_source_gap",
            {
                "status": source_2438i.get("status"),
                "next_route": source_2438i.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "persistent_blocker_condition_is_zero_zero_three",
            _persistent_escalation_required(source_2438i),
            "pass_fail_blocked_not_zero_zero_three",
            "persistent_blocker_condition_not_met",
            {
                "pass_count": source_2438i.get("candidate_replay_pass_count"),
                "fail_count": source_2438i.get("candidate_replay_fail_count"),
                "blocked_count": source_2438i.get("candidate_replay_blocked_count"),
            },
        ),
        _requirement(
            "source_2438b_engine_closure_ready",
            _source_2438b_ready(source_2438b),
            "source_2438b_engine_closure_not_ready",
            "closure_history_gap",
            {"status": source_2438b.get("status")},
        ),
        _requirement(
            "source_2438d_output_closure_ready",
            _source_2438d_ready(source_2438d),
            "source_2438d_output_closure_not_ready",
            "closure_history_gap",
            {"status": source_2438d.get("status")},
        ),
        _requirement(
            "source_2438f_candidate_level_closure_ready",
            _source_2438f_ready(source_2438f),
            "source_2438f_candidate_level_closure_not_ready",
            "closure_history_gap",
            {"status": source_2438f.get("status")},
        ),
        _requirement(
            "source_2438h_remaining_closure_ready",
            _source_2438h_ready(source_2438h),
            "source_2438h_remaining_closure_not_ready",
            "closure_history_gap",
            {"status": source_2438h.get("status")},
        ),
        _requirement(
            "closure_history_confirmed",
            _closure_history_confirmed(closure_history),
            "closure_history_not_fully_confirmed",
            "closure_history_gap",
            dict(closure_history),
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_gap",
            None,
            {"data_quality_status": data_quality_summary.get("data_quality_status")},
        ),
        _requirement(
            "candidate_replay_output_records_complete",
            len(records) == 3
            and len(set(record_ids)) == 3
            and _candidate_outputs_complete(source_2438d, {}, source_2438i),
            "candidate_output_records_incomplete",
            "candidate_replay_output_record_gap",
            {"record_ids": record_ids, "record_count": len(records)},
        ),
        _requirement(
            "persistent_blocker_records_complete",
            len(persistent_blockers) == 3 and set(blocker_ids) == set(record_ids),
            "persistent_blocker_records_incomplete",
            "persistent_blocker_summary_gap",
            {"blocker_ids": blocker_ids, "record_ids": record_ids},
        ),
        _requirement(
            "escalation_record_count",
            len(escalation_records) == 3 and set(escalation_ids) == set(record_ids),
            "escalation_record_count_not_three",
            "persistent_escalation_record_gap",
            {"escalation_ids": escalation_ids, "record_ids": record_ids},
        ),
        _requirement(
            "all_escalation_records_have_blocker_reason",
            all(bool(record.get("persistent_blocker_reason")) for record in escalation_records),
            "persistent_blocker_reason_missing",
            "persistent_escalation_record_gap",
            {"candidate_ids": escalation_ids},
        ),
        _requirement(
            "all_escalation_records_have_root_cause_category",
            all(bool(record.get("persistent_blocker_category")) for record in escalation_records),
            "root_cause_category_missing",
            "persistent_escalation_record_gap",
            {"candidate_ids": escalation_ids},
        ),
        _requirement(
            "all_escalation_records_have_root_cause_layer",
            all(bool(record.get("root_cause_layer")) for record in escalation_records),
            "root_cause_layer_missing",
            "persistent_escalation_record_gap",
            {"candidate_ids": escalation_ids},
        ),
        _requirement(
            "all_escalation_records_have_recommended_next_action",
            all(bool(record.get("recommended_next_action")) for record in escalation_records),
            "recommended_next_action_missing",
            "persistent_escalation_record_gap",
            {"candidate_ids": escalation_ids},
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


def _candidate_records(
    candidate_replay_output_records: Mapping[str, Any],
    source_2438i: Mapping[str, Any],
    source_2438d: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (candidate_replay_output_records, source_2438d, source_2438i):
        section = _mapping(source.get("candidate_replay_output_records"))
        records = _sequence(section.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _persistent_blockers(
    persistent_summary: Mapping[str, Any],
    source_2438i: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (persistent_summary, source_2438i):
        section = _mapping(source.get("persistent_candidate_replay_blocker_summary"))
        records = _sequence(section.get("persistent_candidate_replay_blockers"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("persistent_candidate_replay_blockers"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _normalize_candidate_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized["replay_status"] = str(normalized.get("replay_status", "")).upper()
    normalized["status_reason"] = dict(_mapping(normalized.get("status_reason")))
    normalized["paper_shadow_candidate_found"] = False
    normalized["paper_shadow_enabled"] = False
    normalized["trading_advice_generated"] = False
    normalized["broker_order_generated"] = False
    normalized["portfolio_weight_mutated"] = False
    normalized["production_effect"] = "none"
    normalized["broker_action"] = "none"
    return normalized


def _closure_history(
    source_2438b: Mapping[str, Any],
    source_2438d: Mapping[str, Any],
    source_2438f: Mapping[str, Any],
    source_2438h: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "pit_replay_engine_blocker_closure_ready": _source_2438b_ready(source_2438b),
        "output_completeness_closure_ready": _source_2438d_ready(source_2438d),
        "candidate_level_blocker_closure_ready": _source_2438f_ready(source_2438f),
        "remaining_blocker_closure_ready": _source_2438h_ready(source_2438h),
        "repeated_closure_attempt_count": sum(
            (
                _source_2438b_ready(source_2438b),
                _source_2438d_ready(source_2438d),
                _source_2438f_ready(source_2438f),
                _source_2438h_ready(source_2438h),
            )
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _escalation_records(
    candidate_records: Sequence[Mapping[str, Any]],
    persistent_blockers: Sequence[Mapping[str, Any]],
    closure_history: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers_by_id = {
        str(blocker.get("candidate_id")): blocker
        for blocker in persistent_blockers
        if blocker.get("candidate_id")
    }
    records: list[dict[str, Any]] = []
    for candidate in candidate_records:
        if str(candidate.get("replay_status", "")).upper() != "BLOCKED":
            continue
        candidate_id = str(candidate.get("candidate_id") or "")
        blocker = _mapping(blockers_by_id.get(candidate_id))
        root_causes = _root_cause_categories(candidate, blocker, closure_history)
        primary = _primary_root_cause(root_causes)
        root_layers = _root_cause_layers(root_causes)
        recommended_action = _recommended_action(primary)
        status_reason = _mapping(candidate.get("status_reason"))
        records.append(
            {
                "candidate_id": candidate_id,
                "prior_replay_status": "BLOCKED",
                "closure_history": dict(closure_history),
                "latest_recheck_status": "BLOCKED",
                "persistent_blocker_category": primary,
                "persistent_blocker_root_cause_categories": root_causes,
                "persistent_blocker_reason": (
                    blocker.get("blocker_reason")
                    or status_reason.get("blocker_reason")
                ),
                "repeated_closure_attempt_count": closure_history.get(
                    "repeated_closure_attempt_count"
                ),
                "why_previous_closure_was_insufficient": (
                    "Prior closure artifacts reached READY, but the latest candidate "
                    "replay output still has replay_status=BLOCKED and lacks "
                    "materialized executable replay metrics or outcome evidence."
                ),
                "root_cause_layer": root_layers,
                "recommended_next_action": recommended_action,
                "source_replay_status": candidate.get("source_replay_status"),
                "blocking_gap_ids": list(_sequence(candidate.get("blocking_gap_ids"))),
                "metric_summary": dict(_mapping(candidate.get("metric_summary"))),
                "evidence_ref": candidate.get("evidence_ref"),
                "closure_evidence_ref": blocker.get("closure_evidence_ref"),
                "replay_outcome_after_escalation": "NOT_RECHECKED",
                "eligible_for_forward_aging": False,
                "paper_shadow_candidate_found": False,
                "paper_shadow_enabled": False,
                "trading_advice_generated": False,
                "broker_order_generated": False,
                "portfolio_weight_mutated": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return records


def _root_cause_categories(
    candidate: Mapping[str, Any],
    blocker: Mapping[str, Any],
    closure_history: Mapping[str, Any],
) -> list[str]:
    blocker_categories = {
        str(category)
        for category in _sequence(blocker.get("blocker_category"))
        if category
    }
    gaps = {str(gap).lower() for gap in _sequence(candidate.get("blocking_gap_ids"))}
    root_causes: list[str] = []
    for category in blocker_categories:
        if category in ROOT_CAUSE_CATEGORIES and category not in root_causes:
            root_causes.append(category)
    if "replay_engine_execution_gap" in blocker_categories or any(
        "engine" in gap for gap in gaps
    ):
        if closure_history.get("pit_replay_engine_blocker_closure_ready") is True:
            root_causes.append("replay_engine_contract_ready_but_runtime_not_executable")
        else:
            root_causes.append("replay_execution_not_materialized")
    metric_summary = _mapping(candidate.get("metric_summary"))
    if "missing_metric_summary" in blocker_categories or _metric_summary_empty(
        metric_summary
    ):
        root_causes.append("candidate_metric_materialization_missing")
    if not candidate.get("baseline_id"):
        root_causes.append("baseline_comparison_not_materialized")
    if "unresolved_input_dependency" in blocker_categories or any(
        "input" in gap for gap in gaps
    ):
        root_causes.append("candidate_input_spec_semantically_incomplete")
    if "insufficient_pit_window" in blocker_categories or any(
        "as_of" in gap or "valid_until" in gap for gap in gaps
    ):
        root_causes.append("candidate_replay_window_unresolvable")
    if "unresolved_source_traceability" in blocker_categories:
        root_causes.append("candidate_evidence_chain_incomplete_despite_closure")
    if "missing_outcome_linkage" in blocker_categories or any(
        "outcome" in gap for gap in gaps
    ):
        root_causes.append("outcome_linkage_not_materialized")
    if "missing_forward_aging_handoff_key" in blocker_categories:
        root_causes.append("forward_aging_handoff_not_materialized")
    ordered = [category for category in ROOT_CAUSE_CATEGORIES if category in root_causes]
    return ordered or ["other"]


def _primary_root_cause(root_causes: Sequence[str]) -> str:
    priority = (
        "replay_engine_contract_ready_but_runtime_not_executable",
        "replay_execution_not_materialized",
        "candidate_metric_materialization_missing",
        "candidate_input_spec_semantically_incomplete",
        "candidate_replay_window_unresolvable",
        "outcome_linkage_not_materialized",
        "candidate_evidence_chain_incomplete_despite_closure",
        "forward_aging_handoff_not_materialized",
        "baseline_comparison_not_materialized",
        "pass_fail_threshold_not_executable",
        "candidate_definition_not_replayable",
        "top3_selection_artifact_not_sufficient_for_replay",
        "other",
    )
    root_set = set(root_causes)
    for category in priority:
        if category in root_set:
            return category
    return "other"


def _root_cause_layers(root_causes: Sequence[str]) -> list[str]:
    layers: list[str] = []
    mapping = {
        "replay_engine_contract_ready_but_runtime_not_executable": "engine_runtime",
        "replay_execution_not_materialized": "engine_runtime",
        "candidate_metric_materialization_missing": "metric_materialization",
        "baseline_comparison_not_materialized": "metric_materialization",
        "pass_fail_threshold_not_executable": "threshold_definition",
        "candidate_evidence_chain_incomplete_despite_closure": "evidence_materialization",
        "candidate_replay_window_unresolvable": "candidate_spec",
        "candidate_input_spec_semantically_incomplete": "candidate_spec",
        "outcome_linkage_not_materialized": "outcome_linkage",
        "forward_aging_handoff_not_materialized": "forward_aging_handoff",
        "candidate_definition_not_replayable": "candidate_spec",
        "top3_selection_artifact_not_sufficient_for_replay": "artifact_contract",
        "other": "unknown",
    }
    for root_cause in root_causes:
        layer = mapping.get(root_cause, "unknown")
        if layer not in layers:
            layers.append(layer)
    return layers


def _recommended_action(primary_root_cause: str) -> str:
    if primary_root_cause in {
        "replay_engine_contract_ready_but_runtime_not_executable",
        "replay_execution_not_materialized",
    }:
        return "replay_runtime_materialization_remediation"
    if primary_root_cause in {
        "candidate_metric_materialization_missing",
        "baseline_comparison_not_materialized",
        "pass_fail_threshold_not_executable",
    }:
        return "candidate_metric_materialization_remediation"
    if primary_root_cause in {
        "candidate_input_spec_semantically_incomplete",
        "candidate_replay_window_unresolvable",
        "candidate_definition_not_replayable",
    }:
        return "candidate_spec_rebuild"
    if primary_root_cause == "top3_selection_artifact_not_sufficient_for_replay":
        return "top3_candidate_reselection"
    if primary_root_cause == "other":
        return "persistent_blocker_manual_review"
    return "persistent_blocker_manual_review"


def _root_cause_rows(
    escalation_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": record.get("candidate_id"),
            "root_cause_category": record.get("persistent_blocker_category"),
            "root_cause_categories": list(
                _sequence(record.get("persistent_blocker_root_cause_categories"))
            ),
            "root_cause_layer": list(_sequence(record.get("root_cause_layer"))),
            "recommended_next_action": record.get("recommended_next_action"),
            "persistent_blocker_reason": record.get("persistent_blocker_reason"),
            "replay_outcome_after_escalation": "NOT_RECHECKED",
            "eligible_for_forward_aging": False,
            "paper_shadow_candidate_found": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        for record in escalation_records
    ]


def _root_cause_matrix(
    status: str,
    rows: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": ROOT_CAUSE_MATRIX_SCHEMA_VERSION,
        "status": status,
        "root_cause_matrix_ready": status == READY_STATUS,
        "persistent_blocked_candidate_count": len(rows),
        "rows": list(rows),
        "next_route": next_route,
        "forward_aging_handoff_ready": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _repeated_closure_failure_summary(
    status: str,
    closure_history: Mapping[str, Any],
    escalation_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": REPEATED_CLOSURE_SCHEMA_VERSION,
        "status": status,
        "repeated_closure_failure_summary_ready": status == READY_STATUS,
        "closure_history_confirmed": _closure_history_confirmed(closure_history),
        "closure_history": dict(closure_history),
        "persistent_blocked_candidate_count": len(escalation_records),
        "candidate_replay_pass_count": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": len(escalation_records),
        "why_previous_closures_were_insufficient": [
            record.get("why_previous_closure_was_insufficient")
            for record in escalation_records
        ],
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _recommended_remediation_route(
    status: str,
    rows: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": REMEDIATION_ROUTE_SCHEMA_VERSION,
        "status": status,
        "recommended_remediation_route_ready": status == READY_STATUS,
        "next_route": next_route,
        "route_reason": _route_reason(next_route, rows),
        "root_cause_rows": list(rows),
        "forward_aging_handoff_ready": False,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_forward_aging_safety_decision(
    status: str,
    escalation_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": NO_FORWARD_AGING_SCHEMA_VERSION,
        "status": status,
        "no_forward_aging_safety_decision_ready": True,
        "forward_aging_handoff_ready": False,
        "forward_aging_candidate_count": 0,
        "persistent_blocked_candidate_count": len(escalation_records),
        "decision_reason": (
            "Persistent candidate replay blockers remain after repeated closure "
            "attempts; forward-aging stays disabled until 2438K remediation."
        ),
        "next_route": next_route,
        "paper_shadow_candidate_found": False,
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


def _recommended_next_route(rows: Sequence[Mapping[str, Any]]) -> str:
    categories = {str(row.get("root_cause_category")) for row in rows}
    if categories & set(ROOT_CAUSE_ROUTE_DEFINITION_REBUILD):
        return NEXT_ROUTE_DEFINITION_REBUILD
    if "other" in categories or not rows:
        return NEXT_ROUTE_MANUAL_REVIEW
    return NEXT_ROUTE_ROOT_CAUSE_REMEDIATION


def _route_reason(next_route: str, rows: Sequence[Mapping[str, Any]]) -> str:
    if next_route == NEXT_ROUTE_DEFINITION_REBUILD:
        return (
            "At least one persistent blocker indicates the top-3 candidate "
            "definition or selection artifact is not replayable."
        )
    if next_route == NEXT_ROUTE_MANUAL_REVIEW:
        return (
            "Escalation evidence is incomplete or contains unknown root causes; "
            "manual review is required before remediation."
        )
    categories = sorted({str(row.get("root_cause_category")) for row in rows})
    return (
        "Persistent blockers are classified for root-cause remediation: "
        + ", ".join(categories)
    )


def _source_2438i_ready(source_2438i: Mapping[str, Any]) -> bool:
    next_route = source_2438i.get("recommended_next_research_task") or source_2438i.get(
        "next_route"
    )
    return source_2438i.get("status") == EXPECTED_2438I_STATUS and next_route == (
        EXPECTED_2438I_ROUTE
    )


def _persistent_escalation_required(source_2438i: Mapping[str, Any]) -> bool:
    return (
        _source_2438i_ready(source_2438i)
        and _int(source_2438i.get("candidate_replay_pass_count"), -1) == 0
        and _int(source_2438i.get("candidate_replay_fail_count"), -1) == 0
        and _int(source_2438i.get("candidate_replay_blocked_count"), -1) == 3
        and _int(source_2438i.get("persistent_candidate_replay_blocker_count"), -1) == 3
    )


def _source_2438b_ready(source_2438b: Mapping[str, Any]) -> bool:
    return (
        source_2438b.get("status") == EXPECTED_2438B_STATUS
        and source_2438b.get("blocker_closure_ready") is True
        and source_2438b.get("pit_replay_engine_ready") is True
        and _int(source_2438b.get("blocker_count_after"), 0) == 0
    )


def _source_2438d_ready(source_2438d: Mapping[str, Any]) -> bool:
    return (
        source_2438d.get("status") == EXPECTED_2438D_STATUS
        and source_2438d.get("blocker_closure_ready") is True
        and source_2438d.get("candidate_replay_outputs_complete") is True
        and _int(source_2438d.get("candidate_replay_output_record_count"), 0) == 3
    )


def _source_2438f_ready(source_2438f: Mapping[str, Any]) -> bool:
    return (
        source_2438f.get("status") == EXPECTED_2438F_STATUS
        and source_2438f.get("candidate_level_blocker_closure_ready") is True
        and _int(source_2438f.get("candidate_level_blocker_count_after"), 0) == 0
    )


def _source_2438h_ready(source_2438h: Mapping[str, Any]) -> bool:
    return (
        source_2438h.get("status") == EXPECTED_2438H_STATUS
        and source_2438h.get("remaining_candidate_blocker_closure_ready") is True
        and _int(source_2438h.get("remaining_candidate_blocker_count_after"), 0) == 0
    )


def _closure_history_confirmed(closure_history: Mapping[str, Any]) -> bool:
    return (
        closure_history.get("pit_replay_engine_blocker_closure_ready") is True
        and closure_history.get("output_completeness_closure_ready") is True
        and closure_history.get("candidate_level_blocker_closure_ready") is True
        and closure_history.get("remaining_blocker_closure_ready") is True
    )


def _candidate_outputs_complete(
    source_2438d: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    source_2438i: Mapping[str, Any],
) -> bool:
    section = _mapping(candidate_replay_output_records.get("candidate_replay_output_records"))
    return (
        source_2438d.get("candidate_replay_outputs_complete") is True
        or source_2438i.get("candidate_replay_outputs_complete") is True
        or section.get("candidate_replay_output_records_ready") is True
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", "")).upper()
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _metric_summary_empty(metric_summary: Mapping[str, Any]) -> bool:
    return not metric_summary or all(metric_summary.get(key) is None for key in METRIC_KEYS)


def _candidate_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        str(record.get("candidate_id"))
        for record in records
        if record.get("candidate_id")
    ]


def _replay_status_count(records: Sequence[Mapping[str, Any]], replay_status: str) -> int:
    return sum(
        1
        for record in records
        if str(record.get("replay_status", "")).upper() == replay_status
    )


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


def _docs_cover_escalation(text: str) -> bool:
    lowered = text.lower()
    return (
        "2438j" in lowered
        and "persistent" in lowered
        and "root" in lowered
        and "blocked" in lowered
        and "forward-aging" in lowered
    )


def _next_route_reason(status: str, next_route: str) -> str:
    if status == READY_STATUS:
        return _route_reason(next_route, [])
    return (
        "Persistent blocker escalation evidence is incomplete; route to manual "
        "review before remediation, forward-aging, or paper-shadow."
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return ()


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
