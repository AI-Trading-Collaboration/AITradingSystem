from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = (
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation.v1"
)
RUNTIME_MATERIALIZATION_SCHEMA_VERSION = (
    "growth_tilt_candidate_replay_runtime_materialization_remediation.v1"
)
RUNTIME_BEFORE_AFTER_SCHEMA_VERSION = "growth_tilt_replay_runtime_before_after_matrix.v1"
EXECUTABLE_REPLAY_HANDOFF_SCHEMA_VERSION = (
    "growth_tilt_executable_replay_readiness_handoff.v1"
)
REMAINING_RUNTIME_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_remaining_replay_runtime_blocker_summary.v1"
)
RUNTIME_EXECUTION_AUDIT_SCHEMA_VERSION = "growth_tilt_runtime_execution_audit_trail.v1"

READY_STATUS = (
    "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_"
    "REMEDIATION_READY"
)
BLOCKED_STATUS = (
    "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_"
    "REMEDIATION_BLOCKED"
)

EXPECTED_2438J_STATUS = (
    "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ESCALATION_READY"
)
EXPECTED_2438J_ROUTE = (
    "TRADING-2438K_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_"
    "Root_Cause_Remediation"
)
EXPECTED_2438I_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_"
    "BLOCKER_CLOSURE_BLOCKED"
)
EXPECTED_2438H_STATUS = "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY"
EXPECTED_2438F_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY"
)
EXPECTED_2438D_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY"
EXPECTED_2438B_STATUS = "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY"
EXPECTED_ROOT_CAUSE = "replay_engine_contract_ready_but_runtime_not_executable"

NEXT_ROUTE_RECHECK_AFTER_RUNTIME_REMEDIATION = (
    "TRADING-2438L_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_"
    "Runtime_Remediation"
)
NEXT_ROUTE_RUNTIME_BLOCKER_CONTINUATION = (
    "TRADING-2438L_Growth_Tilt_Persistent_Replay_Runtime_Blocker_"
    "Remediation_Continuation"
)
REPORT_TYPE = "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation"

METRIC_KEYS: tuple[str, ...] = (
    "return_delta_vs_baseline",
    "max_drawdown_delta_vs_baseline",
    "turnover_delta_vs_baseline",
    "false_risk_off_delta",
    "missed_upside_delta",
    "whipsaw_delta",
)
RUNTIME_REQUIREMENT_IDS: tuple[str, ...] = (
    "candidate_spec_to_runtime_input_adapter_ready",
    "replay_runtime_entrypoint_ready",
    "replay_window_materialization_ready",
    "baseline_comparison_runtime_ready",
    "metric_materialization_runtime_ready",
    "pass_fail_threshold_evaluator_ready",
    "source_traceability_runtime_bindings_ready",
    "as_of_boundary_enforced_at_runtime",
    "valid_until_policy_bound_at_runtime",
    "outcome_linkage_key_runtime_bound",
    "forward_aging_handoff_key_runtime_bound",
    "execution_audit_trail_ready",
    "deterministic_runtime_output_supported",
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_persistent_candidate_pit_replay_blocker_escalation",
    "growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure",
    "growth_tilt_remaining_candidate_pit_replay_blocker_closure",
    "growth_tilt_top3_candidate_level_pit_replay_blocker_closure",
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure",
    "growth_tilt_pit_replay_engine_blocker_closure",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-persistent-candidate-pit-replay-blocker-root-cause-remediation",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/"
    "root_cause_remediation_result.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/"
    "runtime_materialization_remediation.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/"
    "runtime_before_after_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/"
    "executable_replay_readiness_handoff.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/"
    "remaining_runtime_blocker_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/"
    "runtime_execution_audit_trail.json",
    "docs/research/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation.md",
    "docs/research/growth_tilt_candidate_replay_runtime_materialization.md",
    "docs/research/growth_tilt_runtime_before_after_matrix.md",
    "docs/research/growth_tilt_executable_replay_readiness_handoff.md",
    "docs/research/growth_tilt_remaining_replay_runtime_blockers.md",
    "docs/research/growth_tilt_runtime_execution_audit_trail.md",
    "docs/research/dynamic_strategy_2438L_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-persistent-candidate-pit-replay-blocker-root-cause-remediation",
    READY_STATUS,
    BLOCKED_STATUS,
    EXPECTED_2438J_STATUS,
    EXPECTED_ROOT_CAUSE,
    NEXT_ROUTE_RECHECK_AFTER_RUNTIME_REMEDIATION,
    NEXT_ROUTE_RUNTIME_BLOCKER_CONTINUATION,
    "replay_runtime_materialization_ready",
    "candidate_replay_runtime_executable",
    "NOT_RECHECKED",
)


def build_growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation(
    source_2438j_escalation: Mapping[str, Any],
    root_cause_matrix: Mapping[str, Any],
    source_2438i_blocked_recheck: Mapping[str, Any],
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
    records = [
        _normalize_candidate_record(record)
        for record in _candidate_records(
            candidate_replay_output_records,
            source_2438d_output_closure,
            source_2438i_blocked_recheck,
        )
    ]
    root_cause_rows = _root_cause_rows(source_2438j_escalation, root_cause_matrix)
    root_causes_by_id = {
        str(row.get("candidate_id")): row
        for row in root_cause_rows
        if row.get("candidate_id")
    }
    resolved_as_of = as_of or str(
        source_2438j_escalation.get("as_of")
        or source_2438i_blocked_recheck.get("as_of")
        or _first_record_as_of(records)
        or ""
    )
    runtime_records = [
        _candidate_runtime_record(record, root_causes_by_id, resolved_as_of)
        for record in records
        if str(record.get("replay_status", "")).upper() == "BLOCKED"
    ]
    runtime_requirement_status = _runtime_requirement_status(runtime_records)
    closure_history = _closure_history(
        source_2438b_engine_blocker_closure,
        source_2438d_output_closure,
        source_2438f_candidate_level_blocker_closure,
        source_2438h_remaining_blocker_closure,
    )
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _remediation_requirements(
        source_2438j_escalation,
        root_cause_rows,
        source_2438i_blocked_recheck,
        source_2438h_remaining_blocker_closure,
        source_2438f_candidate_level_blocker_closure,
        source_2438d_output_closure,
        source_2438b_engine_blocker_closure,
        data_quality_summary,
        records,
        runtime_records,
        runtime_requirement_status,
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
    ready = not gaps
    status = READY_STATUS if ready else BLOCKED_STATUS
    next_route = (
        NEXT_ROUTE_RECHECK_AFTER_RUNTIME_REMEDIATION
        if ready
        else NEXT_ROUTE_RUNTIME_BLOCKER_CONTINUATION
    )
    remaining_records = [
        record for record in runtime_records if record["runtime_remediation_ready"] is not True
    ]
    pass_count = _replay_status_count(records, "PASS")
    fail_count = _replay_status_count(records, "FAIL")
    blocked_count = _replay_status_count(records, "BLOCKED")
    runtime_materialization = _runtime_materialization_remediation(
        status,
        runtime_records,
        runtime_requirement_status,
        next_route,
    )
    before_after = _runtime_before_after_matrix(status, runtime_records, next_route)
    handoff = _executable_replay_readiness_handoff(
        status,
        runtime_records,
        next_route,
    )
    blocker_summary = _remaining_runtime_blocker_summary(
        status,
        remaining_records,
        next_route,
    )
    audit_trail = _runtime_execution_audit_trail(status, runtime_records, next_route)

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438K",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": (
            "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_"
            "Blocker_Escalation"
        ),
        "prior_status": source_2438j_escalation.get("status"),
        "expected_prior_root_cause": EXPECTED_ROOT_CAUSE,
        "source_2438j_escalation_ready": _source_2438j_ready(source_2438j_escalation),
        "source_2438i_blocked_recheck_ready": _source_2438i_ready(
            source_2438i_blocked_recheck
        ),
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
        "closure_history_confirmed": _closure_history_confirmed(closure_history),
        "prior_root_cause_matched": _prior_root_cause_matched(root_cause_rows),
        "root_cause_remediation_ready": ready,
        "replay_runtime_materialization_ready": ready,
        "candidate_replay_runtime_executable": ready,
        "candidate_replay_runtime_executable_count": sum(
            1 for record in runtime_records if record["runtime_remediation_ready"] is True
        ),
        "runtime_blocker_count_before": blocked_count,
        "runtime_blocker_count_after": len(remaining_records),
        "candidate_replay_pass_count": pass_count,
        "candidate_replay_fail_count": fail_count,
        "candidate_replay_blocked_count": blocked_count,
        "candidate_replay_outcome_rechecked": False,
        "replay_outcome_after_remediation": "NOT_RECHECKED",
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "candidate_replay_output_record_count": len(records),
        "candidate_replay_outputs_complete": _candidate_outputs_complete(
            source_2438d_output_closure,
            candidate_replay_output_records,
            source_2438i_blocked_recheck,
        ),
        "candidate_runtime_remediation_records": runtime_records,
        "runtime_materialization_remediation": runtime_materialization,
        "runtime_before_after_matrix": before_after,
        "executable_replay_readiness_handoff": handoff,
        "remaining_runtime_blocker_summary": blocker_summary,
        "runtime_execution_audit_trail": audit_trail,
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [str(gap["requirement_id"]) for gap in gaps],
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
        "recommended_next_research_task_reason": _next_route_reason(status),
    }
    payload.update(runtime_requirement_status)
    return payload


def _remediation_requirements(
    source_2438j: Mapping[str, Any],
    root_cause_rows: Sequence[Mapping[str, Any]],
    source_2438i: Mapping[str, Any],
    source_2438h: Mapping[str, Any],
    source_2438f: Mapping[str, Any],
    source_2438d: Mapping[str, Any],
    source_2438b: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    runtime_records: Sequence[Mapping[str, Any]],
    runtime_requirement_status: Mapping[str, bool],
    closure_history: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    record_ids = _candidate_ids(records)
    runtime_ids = _candidate_ids(runtime_records)
    root_cause_ids = _candidate_ids(root_cause_rows)
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_runtime_remediation(research_text)
    )
    requirements = [
        _requirement(
            "source_2438j_escalation_ready",
            _source_2438j_ready(source_2438j),
            "source_2438j_not_ready_for_runtime_remediation",
            "source_2438j_gap",
            {
                "status": source_2438j.get("status"),
                "next_route": source_2438j.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "source_2438i_blocked_recheck_ready",
            _source_2438i_ready(source_2438i),
            "source_2438i_not_persistent_blocked",
            "source_2438i_gap",
            {"status": source_2438i.get("status")},
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
            and all(
                str(record.get("replay_status", "")).upper() == "BLOCKED"
                for record in records
            ),
            "candidate_output_records_incomplete_or_not_blocked",
            "candidate_replay_output_record_gap",
            {"record_ids": record_ids, "record_count": len(records)},
        ),
        _requirement(
            "candidate_root_cause_records_complete",
            len(root_cause_rows) == 3
            and len(set(root_cause_ids)) == 3
            and set(root_cause_ids) == set(record_ids),
            "root_cause_records_not_aligned_to_candidates",
            "root_cause_matrix_gap",
            {"root_cause_ids": root_cause_ids, "record_ids": record_ids},
        ),
        _requirement(
            "prior_root_cause_matched",
            _prior_root_cause_matched(root_cause_rows),
            "prior_root_cause_not_runtime_executable_gap",
            "root_cause_route_gap",
            {"expected_root_cause": EXPECTED_ROOT_CAUSE, "root_cause_ids": root_cause_ids},
        ),
        _requirement(
            "candidate_runtime_remediation_records_complete",
            len(runtime_records) == 3
            and len(set(runtime_ids)) == 3
            and set(runtime_ids) == set(record_ids),
            "runtime_remediation_records_incomplete",
            "runtime_remediation_record_gap",
            {"runtime_ids": runtime_ids, "record_ids": record_ids},
        ),
    ]
    requirements.extend(
        _requirement(
            requirement_id,
            runtime_requirement_status.get(requirement_id) is True,
            f"{requirement_id}_missing",
            "runtime_materialization_gap",
            {"candidate_ids": runtime_ids},
        )
        for requirement_id in RUNTIME_REQUIREMENT_IDS
    )
    requirements.append(
        _requirement(
            "all_runtime_smoke_checks_passed",
            all(
                record.get("runtime_execution_smoke_check_status") == "PASS"
                for record in runtime_records
            )
            and len(runtime_records) == 3,
            "runtime_smoke_check_not_passed",
            "runtime_smoke_check_gap",
            {
                "smoke_statuses": [
                    record.get("runtime_execution_smoke_check_status")
                    for record in runtime_records
                ],
            },
        )
    )
    requirements.append(
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
        )
    )
    return requirements


def _candidate_runtime_record(
    candidate: Mapping[str, Any],
    root_causes_by_id: Mapping[str, Mapping[str, Any]],
    as_of: str,
) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or "")
    root_cause = _mapping(root_causes_by_id.get(candidate_id))
    metric_summary = dict(_mapping(candidate.get("metric_summary")))
    runtime_entrypoint_ready = _optional_bool(candidate, "runtime_entrypoint_available")
    threshold_shell_ready = _optional_bool(
        candidate,
        "pass_fail_threshold_evaluator_available",
    )
    deterministic_supported = _optional_bool(
        candidate,
        "deterministic_runtime_output_supported",
    )
    runtime_input_materialized = all(
        (
            candidate_id,
            candidate.get("input_spec_ref"),
            candidate.get("source_traceability_ref"),
            candidate.get("evidence_ref"),
        )
    )
    replay_window_materialized = bool(candidate.get("replay_window") and as_of)
    baseline_materialized = bool(candidate.get("baseline_id"))
    metric_ready = _metric_summary_has_keys(metric_summary)
    threshold_ready = bool(threshold_shell_ready and metric_ready and baseline_materialized)
    source_binding_ready = bool(candidate.get("source_traceability_ref"))
    as_of_enforced = bool(as_of)
    valid_until_bound = bool(candidate.get("valid_until_policy_ref"))
    outcome_bound = bool(candidate.get("outcome_linkage_key"))
    handoff_bound = bool(candidate.get("forward_aging_handoff_key"))
    smoke_ref = f"TRADING-2438K:runtime_smoke_check:{candidate_id}"
    audit_ready = bool(candidate_id and smoke_ref)
    deterministic_ready = bool(
        deterministic_supported
        and runtime_input_materialized
        and replay_window_materialized
        and baseline_materialized
        and metric_ready
        and source_binding_ready
        and valid_until_bound
        and outcome_bound
        and handoff_bound
    )
    checks = {
        "candidate_spec_to_runtime_input_adapter_ready": runtime_input_materialized,
        "replay_runtime_entrypoint_ready": runtime_entrypoint_ready,
        "replay_window_materialization_ready": replay_window_materialized,
        "baseline_comparison_runtime_ready": baseline_materialized,
        "metric_materialization_runtime_ready": metric_ready,
        "pass_fail_threshold_evaluator_ready": threshold_ready,
        "source_traceability_runtime_bindings_ready": source_binding_ready,
        "as_of_boundary_enforced_at_runtime": as_of_enforced,
        "valid_until_policy_bound_at_runtime": valid_until_bound,
        "outcome_linkage_key_runtime_bound": outcome_bound,
        "forward_aging_handoff_key_runtime_bound": handoff_bound,
        "execution_audit_trail_ready": audit_ready,
        "deterministic_runtime_output_supported": deterministic_ready,
    }
    forced_status = str(candidate.get("runtime_smoke_check_forced_status") or "").upper()
    if forced_status in {"PASS", "FAIL", "BLOCKED"}:
        smoke_status = forced_status
    elif all(checks.values()):
        smoke_status = "PASS"
    else:
        smoke_status = "BLOCKED"
    missing_ids = [
        requirement_id
        for requirement_id, passed in checks.items()
        if passed is not True
    ]
    if smoke_status != "PASS" and "runtime_execution_smoke_check_passed" not in missing_ids:
        missing_ids.append("runtime_execution_smoke_check_passed")
    runtime_ready = not missing_ids and smoke_status == "PASS"
    return {
        "candidate_id": candidate_id,
        "prior_replay_status": "BLOCKED",
        "prior_root_cause": root_cause.get("root_cause_category")
        or root_cause.get("persistent_blocker_category"),
        "prior_root_cause_categories": list(
            _sequence(root_cause.get("root_cause_categories"))
        ),
        "runtime_input_materialized": runtime_input_materialized,
        "replay_window_materialized": replay_window_materialized,
        "baseline_comparison_materialized": baseline_materialized,
        "metric_materialization_ready": metric_ready,
        "pass_fail_threshold_evaluator_ready": threshold_ready,
        "source_traceability_runtime_binding_ready": source_binding_ready,
        "as_of_boundary_enforced": as_of_enforced,
        "valid_until_policy_bound": valid_until_bound,
        "outcome_linkage_key_bound": outcome_bound,
        "forward_aging_handoff_key_bound": handoff_bound,
        "runtime_execution_smoke_check_status": smoke_status,
        "runtime_execution_smoke_check_ref": smoke_ref,
        "runtime_remediation_ready": runtime_ready,
        "remaining_runtime_blocker_ids": missing_ids,
        "remaining_runtime_blocker_reason": None
        if runtime_ready
        else ", ".join(missing_ids),
        "replay_outcome_after_remediation": "NOT_RECHECKED",
        "eligible_for_forward_aging": False,
        "paper_shadow_candidate_found": False,
        "trading_advice_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "runtime_entrypoint_ref": "TRADING-2438K:replay_runtime_entrypoint_shell",
        "runtime_input_ref": f"TRADING-2438K:runtime_input:{candidate_id}",
        "replay_window_ref": candidate.get("replay_window"),
        "baseline_comparison_ref": candidate.get("baseline_id"),
        "metric_materialization_ref": f"TRADING-2438K:metric_materialization:{candidate_id}",
        "threshold_evaluator_ref": "TRADING-2438K:pass_fail_threshold_evaluator_shell",
        "source_traceability_runtime_ref": candidate.get("source_traceability_ref"),
        "valid_until_policy_ref": candidate.get("valid_until_policy_ref"),
        "outcome_linkage_key": candidate.get("outcome_linkage_key"),
        "forward_aging_handoff_key": candidate.get("forward_aging_handoff_key"),
        "metric_summary": metric_summary,
        "as_of": as_of,
        "production_effect": "none",
        "broker_action": "none",
        **checks,
    }


def _runtime_requirement_status(
    runtime_records: Sequence[Mapping[str, Any]],
) -> dict[str, bool]:
    return {
        requirement_id: bool(runtime_records)
        and all(record.get(requirement_id) is True for record in runtime_records)
        for requirement_id in RUNTIME_REQUIREMENT_IDS
    }


def _runtime_materialization_remediation(
    status: str,
    runtime_records: Sequence[Mapping[str, Any]],
    runtime_requirement_status: Mapping[str, bool],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_MATERIALIZATION_SCHEMA_VERSION,
        "status": status,
        "runtime_materialization_remediation_ready": status == READY_STATUS,
        "replay_runtime_materialization_ready": status == READY_STATUS,
        "candidate_replay_runtime_executable_count": sum(
            1 for record in runtime_records if record.get("runtime_remediation_ready") is True
        ),
        "runtime_requirement_status": dict(runtime_requirement_status),
        "candidate_runtime_remediation_records": list(runtime_records),
        "candidate_replay_outcome_rechecked": False,
        "replay_outcome_after_remediation": "NOT_RECHECKED",
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _runtime_before_after_matrix(
    status: str,
    runtime_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_BEFORE_AFTER_SCHEMA_VERSION,
        "status": status,
        "runtime_before_after_matrix_ready": True,
        "runtime_blocker_count_before": len(runtime_records),
        "runtime_blocker_count_after": sum(
            1 for record in runtime_records if record.get("runtime_remediation_ready") is not True
        ),
        "rows": [
            {
                "candidate_id": record.get("candidate_id"),
                "prior_root_cause": record.get("prior_root_cause"),
                "before_runtime_executable": False,
                "after_runtime_executable": record.get("runtime_remediation_ready"),
                "runtime_execution_smoke_check_status": record.get(
                    "runtime_execution_smoke_check_status"
                ),
                "remaining_runtime_blocker_reason": record.get(
                    "remaining_runtime_blocker_reason"
                ),
                "replay_outcome_after_remediation": "NOT_RECHECKED",
                "eligible_for_forward_aging": False,
                "production_effect": "none",
                "broker_action": "none",
            }
            for record in runtime_records
        ],
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _executable_replay_readiness_handoff(
    status: str,
    runtime_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    ready_records = [
        record for record in runtime_records if record.get("runtime_remediation_ready") is True
    ]
    return {
        "schema_version": EXECUTABLE_REPLAY_HANDOFF_SCHEMA_VERSION,
        "status": status,
        "executable_replay_readiness_handoff_ready": status == READY_STATUS,
        "ready_for_2438l_recheck": status == READY_STATUS,
        "handoff_candidate_count": len(ready_records),
        "handoff_candidates": [
            {
                "candidate_id": record.get("candidate_id"),
                "runtime_execution_smoke_check_ref": record.get(
                    "runtime_execution_smoke_check_ref"
                ),
                "outcome_linkage_key": record.get("outcome_linkage_key"),
                "forward_aging_handoff_key": record.get("forward_aging_handoff_key"),
                "replay_outcome_after_remediation": "NOT_RECHECKED",
                "eligible_for_forward_aging": False,
            }
            for record in ready_records
        ],
        "candidate_replay_outcome_rechecked": False,
        "forward_aging_handoff_ready": False,
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_runtime_blocker_summary(
    status: str,
    remaining_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": REMAINING_RUNTIME_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "remaining_runtime_blocker_summary_ready": True,
        "remaining_runtime_blocker_count": len(remaining_records),
        "remaining_runtime_blockers": [
            {
                "candidate_id": record.get("candidate_id"),
                "remaining_runtime_blocker_ids": list(
                    _sequence(record.get("remaining_runtime_blocker_ids"))
                ),
                "remaining_runtime_blocker_reason": record.get(
                    "remaining_runtime_blocker_reason"
                ),
                "required_next_action": (
                    "Complete runtime materialization before TRADING-2438L replay "
                    "outcome recheck."
                ),
            }
            for record in remaining_records
        ],
        "next_route": next_route,
        "forward_aging_handoff_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _runtime_execution_audit_trail(
    status: str,
    runtime_records: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_EXECUTION_AUDIT_SCHEMA_VERSION,
        "status": status,
        "runtime_execution_audit_trail_ready": status == READY_STATUS,
        "audit_records": [
            {
                "candidate_id": record.get("candidate_id"),
                "runtime_execution_smoke_check_ref": record.get(
                    "runtime_execution_smoke_check_ref"
                ),
                "runtime_execution_smoke_check_status": record.get(
                    "runtime_execution_smoke_check_status"
                ),
                "runtime_input_ref": record.get("runtime_input_ref"),
                "runtime_entrypoint_ref": record.get("runtime_entrypoint_ref"),
                "deterministic_runtime_output_supported": record.get(
                    "deterministic_runtime_output_supported"
                ),
                "candidate_replay_outcome_rechecked": False,
                "production_effect": "none",
                "broker_action": "none",
            }
            for record in runtime_records
        ],
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_records(
    candidate_replay_output_records: Mapping[str, Any],
    source_2438d: Mapping[str, Any],
    source_2438i: Mapping[str, Any],
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


def _root_cause_rows(
    source_2438j: Mapping[str, Any],
    root_cause_matrix: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (root_cause_matrix, source_2438j):
        section = _mapping(source.get("candidate_persistent_blocker_root_cause_matrix"))
        rows = _sequence(section.get("rows"))
        if rows:
            return [row for row in rows if isinstance(row, Mapping)]
        rows = _sequence(source.get("persistent_blocker_root_causes"))
        if rows:
            return [row for row in rows if isinstance(row, Mapping)]
        rows = _sequence(source.get("rows"))
        if rows:
            return [row for row in rows if isinstance(row, Mapping)]
    records = _sequence(source_2438j.get("candidate_persistent_blocker_escalation_records"))
    return [
        {
            "candidate_id": record.get("candidate_id"),
            "root_cause_category": record.get("persistent_blocker_category"),
            "root_cause_categories": record.get(
                "persistent_blocker_root_cause_categories",
                [],
            ),
        }
        for record in records
        if isinstance(record, Mapping)
    ]


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


def _source_2438j_ready(source_2438j: Mapping[str, Any]) -> bool:
    next_route = source_2438j.get("recommended_next_research_task") or source_2438j.get(
        "next_route"
    )
    return (
        source_2438j.get("status") == EXPECTED_2438J_STATUS
        and next_route == EXPECTED_2438J_ROUTE
        and source_2438j.get("persistent_blocker_escalation_ready") is True
    )


def _source_2438i_ready(source_2438i: Mapping[str, Any]) -> bool:
    return (
        source_2438i.get("status") == EXPECTED_2438I_STATUS
        and _int(source_2438i.get("candidate_replay_pass_count"), -1) == 0
        and _int(source_2438i.get("candidate_replay_fail_count"), -1) == 0
        and _int(source_2438i.get("candidate_replay_blocked_count"), -1) == 3
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


def _prior_root_cause_matched(root_cause_rows: Sequence[Mapping[str, Any]]) -> bool:
    return (
        len(root_cause_rows) == 3
        and all(
            str(row.get("root_cause_category") or row.get("persistent_blocker_category"))
            == EXPECTED_ROOT_CAUSE
            for row in root_cause_rows
        )
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", "")).upper()
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _metric_summary_has_keys(metric_summary: Mapping[str, Any]) -> bool:
    return all(key in metric_summary for key in METRIC_KEYS)


def _optional_bool(source: Mapping[str, Any], key: str) -> bool:
    value = source.get(key)
    if value is False:
        return False
    return True


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


def _docs_cover_runtime_remediation(text: str) -> bool:
    lowered = text.lower()
    return (
        "2438k" in lowered
        and "runtime" in lowered
        and "not_rechecked" in lowered
        and "forward-aging" in lowered
    )


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return (
            "Replay runtime materialization is executable for the three persistent "
            "BLOCKED candidates; 2438L must perform the independent outcome recheck."
        )
    return (
        "Replay runtime materialization is still incomplete; continue runtime "
        "remediation before 2438L can recheck outcomes."
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
