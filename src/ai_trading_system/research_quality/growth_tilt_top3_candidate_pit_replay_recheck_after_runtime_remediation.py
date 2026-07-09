from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = (
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation.v1"
)
RUNTIME_AFTER_RECHECK_SCHEMA_VERSION = (
    "growth_tilt_runtime_remediation_after_recheck.v1"
)
DECISION_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_candidate_runtime_recheck_decision_matrix.v1"
)
FORWARD_AGING_HANDOFF_SCHEMA_VERSION = (
    "growth_tilt_candidate_forward_aging_after_runtime_remediation_handoff_summary.v1"
)
POST_RUNTIME_BLOCKER_SCHEMA_VERSION = (
    "growth_tilt_post_runtime_candidate_replay_blocker_summary.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_candidate_recheck_after_runtime_remediation_no_effect.v1"
)

READY_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_"
    "REMEDIATION_READY"
)
NO_PASSING_CANDIDATE_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_"
    "REMEDIATION_NO_PASSING_CANDIDATE"
)
BLOCKED_STATUS = (
    "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_"
    "REMEDIATION_BLOCKED"
)

EXPECTED_2438K_STATUS = (
    "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_"
    "REMEDIATION_READY"
)
EXPECTED_2438K_ROUTE = (
    "TRADING-2438L_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_"
    "Runtime_Remediation"
)
NEXT_ROUTE_READY = (
    "TRADING-2439A_Growth_Tilt_Forward_Aging_Candidate_Pack_Rebuild_After_"
    "PIT_Replay_Recheck"
)
NEXT_ROUTE_NO_PASS = "TRADING-2439A_Growth_Tilt_No_Passing_PIT_Candidate_Evidence_Review"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438M_Growth_Tilt_Post_Runtime_Candidate_PIT_Replay_Blocker_"
    "Resolution"
)
REPORT_TYPE = "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation"

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
    "growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation",
    "growth_tilt_persistent_candidate_pit_replay_blocker_escalation",
    "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-top3-candidate-pit-replay-recheck-after-runtime-remediation",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/"
    "recheck_after_runtime_remediation_result.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/"
    "runtime_remediation_after_recheck.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/"
    "candidate_pass_fail_blocked_decision_matrix.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/"
    "forward_aging_handoff_readiness_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/"
    "post_runtime_candidate_replay_blocker_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/"
    "no_effect_boundary.json",
    "docs/research/"
    "growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation.md",
    "docs/research/growth_tilt_runtime_remediation_after_recheck.md",
    "docs/research/growth_tilt_candidate_runtime_recheck_decision_matrix.md",
    "docs/research/"
    "growth_tilt_candidate_forward_aging_after_runtime_remediation_handoff_summary.md",
    "docs/research/growth_tilt_post_runtime_candidate_replay_blocker_summary.md",
    "docs/research/"
    "growth_tilt_candidate_recheck_after_runtime_remediation_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438M_or_2439A_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-top3-candidate-pit-replay-recheck-after-runtime-remediation",
    READY_STATUS,
    NO_PASSING_CANDIDATE_STATUS,
    BLOCKED_STATUS,
    EXPECTED_2438K_STATUS,
    NEXT_ROUTE_READY,
    NEXT_ROUTE_NO_PASS,
    NEXT_ROUTE_BLOCKED,
    "candidate_replay_outcome_rechecked",
    "post_runtime_candidate_replay_blockers",
)


def build_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation(
    source_2438k_runtime_remediation: Mapping[str, Any],
    executable_replay_readiness_handoff: Mapping[str, Any],
    runtime_materialization_remediation: Mapping[str, Any],
    runtime_execution_audit_trail: Mapping[str, Any],
    candidate_replay_output_records: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    runtime_records = _runtime_records(
        source_2438k_runtime_remediation,
        runtime_materialization_remediation,
    )
    candidate_records = _candidate_records(
        candidate_replay_output_records,
        source_2438k_runtime_remediation,
    )
    runtime_by_id = _by_candidate_id(runtime_records)
    candidate_by_id = _by_candidate_id(candidate_records)
    handoff = _handoff_section(
        executable_replay_readiness_handoff,
        source_2438k_runtime_remediation,
    )
    handoff_candidates = _handoff_candidates(handoff)
    audit = _audit_section(runtime_execution_audit_trail, source_2438k_runtime_remediation)
    audit_records = _audit_records(audit)
    resolved_as_of = as_of or str(
        source_2438k_runtime_remediation.get("as_of")
        or _first_record_as_of(runtime_records)
        or _first_record_as_of(candidate_records)
        or ""
    )
    candidate_ids = _candidate_ids(runtime_records) or _candidate_ids(candidate_records)
    decisions = [
        _candidate_decision(
            candidate_id,
            runtime_by_id.get(candidate_id, {}),
            candidate_by_id.get(candidate_id, {}),
            _mapping(
                {
                    str(candidate.get("candidate_id")): candidate
                    for candidate in handoff_candidates
                    if candidate.get("candidate_id")
                }.get(candidate_id)
            ),
            _mapping(
                {
                    str(record.get("candidate_id")): record
                    for record in audit_records
                    if record.get("candidate_id")
                }.get(candidate_id)
            ),
            resolved_as_of,
        )
        for candidate_id in candidate_ids
    ]
    passing_candidates = _decisions_with_status(decisions, "PASS")
    failed_candidates = _decisions_with_status(decisions, "FAIL")
    blocked_candidates = _decisions_with_status(decisions, "BLOCKED")
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _recheck_requirements(
        source_2438k_runtime_remediation,
        handoff,
        runtime_records,
        candidate_records,
        candidate_replay_output_records,
        decisions,
        data_quality_summary,
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
    source_recheckable = not gaps
    outcome_rechecked = source_recheckable
    status = _status_from_decisions(
        source_recheckable,
        passing_candidates,
        failed_candidates,
        blocked_candidates,
        decisions,
    )
    next_route = _next_route(status)
    forward_aging_handoff_ready = status == READY_STATUS
    forward_aging_candidates = (
        list(passing_candidates) if forward_aging_handoff_ready else []
    )
    post_runtime_blockers = _post_runtime_blockers(blocked_candidates, gaps)
    decision_matrix = _decision_matrix(status, decisions, next_route)
    runtime_after_recheck = _runtime_remediation_after_recheck(
        status,
        source_2438k_runtime_remediation,
        decisions,
        outcome_rechecked,
        next_route,
    )
    handoff_summary = _forward_aging_handoff_summary(
        status,
        forward_aging_handoff_ready,
        forward_aging_candidates,
        blocked_candidates,
        next_route,
    )
    blocker_summary = _post_runtime_blocker_summary(
        status,
        post_runtime_blockers,
        next_route,
    )
    no_effect_boundary = _no_effect_boundary(status, post_runtime_blockers, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438L",
        "status": status,
        "readiness_status": status,
        "as_of": resolved_as_of,
        "prior_route": (
            "TRADING-2438K_Growth_Tilt_Persistent_Candidate_PIT_Replay_"
            "Blocker_Root_Cause_Remediation"
        ),
        "prior_status": source_2438k_runtime_remediation.get("status"),
        "source_2438k_runtime_remediation_ready": _source_2438k_ready(
            source_2438k_runtime_remediation
        ),
        "runtime_remediation_ready": _source_2438k_ready(
            source_2438k_runtime_remediation
        ),
        "runtime_blocker_count_after": _int(
            source_2438k_runtime_remediation.get("runtime_blocker_count_after"),
            -1,
        ),
        "candidate_replay_runtime_executable_count": _int(
            source_2438k_runtime_remediation.get(
                "candidate_replay_runtime_executable_count"
            ),
            0,
        ),
        "executable_replay_readiness_handoff_ready": _handoff_ready(handoff),
        "handoff_candidate_count": len(handoff_candidates),
        "runtime_remediation_record_count": len(runtime_records),
        "runtime_records_complete": len(runtime_records) == 3,
        "candidate_replay_output_record_count": len(candidate_records),
        "candidate_replay_outputs_complete": _candidate_outputs_complete(
            candidate_replay_output_records,
            source_2438k_runtime_remediation,
        ),
        "runtime_metric_materialization_output_ready": all(
            decision["metric_values_materialized"] is True for decision in decisions
        )
        and bool(decisions),
        "baseline_comparison_runtime_output_ready": all(
            decision["baseline_comparison_materialized"] is True
            for decision in decisions
        )
        and bool(decisions),
        "threshold_evaluator_runtime_output_ready": all(
            decision["threshold_evaluation_available"] is True for decision in decisions
        )
        and bool(decisions),
        "candidate_replay_outcome_rechecked": outcome_rechecked,
        "candidate_replay_pass_count": len(passing_candidates),
        "candidate_replay_fail_count": len(failed_candidates),
        "candidate_replay_blocked_count": len(blocked_candidates),
        "passing_candidates": passing_candidates,
        "failed_candidates": failed_candidates,
        "blocked_candidates": blocked_candidates,
        "post_runtime_candidate_replay_blockers": post_runtime_blockers,
        "post_runtime_candidate_replay_blocker_count": len(post_runtime_blockers),
        "forward_aging_handoff_ready": forward_aging_handoff_ready,
        "forward_aging_candidate_count": len(forward_aging_candidates),
        "forward_aging_candidates": forward_aging_candidates,
        "top3_candidate_count": len(candidate_ids),
        "top3_candidate_ids": list(candidate_ids),
        "registry_catalog_docs_alignment": _check_passed(
            requirements,
            "registry_catalog_docs_alignment",
        ),
        "runtime_remediation_after_recheck": runtime_after_recheck,
        "candidate_pass_fail_blocked_decision_matrix": decision_matrix,
        "forward_aging_handoff_readiness_summary": handoff_summary,
        "post_runtime_candidate_replay_blocker_summary": blocker_summary,
        "no_effect_boundary": no_effect_boundary,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [str(gap["requirement_id"]) for gap in gaps],
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
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
    source_2438k: Mapping[str, Any],
    handoff: Mapping[str, Any],
    runtime_records: Sequence[Mapping[str, Any]],
    candidate_records: Sequence[Mapping[str, Any]],
    candidate_replay_output_records: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    runtime_ids = _candidate_ids(runtime_records)
    candidate_ids = _candidate_ids(candidate_records)
    decision_ids = _candidate_ids(decisions)
    handoff_ids = {
        str(candidate.get("candidate_id"))
        for candidate in _handoff_candidates(handoff)
        if candidate.get("candidate_id")
    }
    record_count_ready = len(candidate_ids) == 3 and len(set(candidate_ids)) == 3
    runtime_records_ready = len(runtime_ids) == 3 and len(set(runtime_ids)) == 3
    registry_catalog_docs_alignment = (
        _report_registry_has(report_registry, REQUIRED_REPORT_IDS)
        and _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES)
        and _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES)
        and _docs_cover_runtime_recheck(research_text)
    )
    return [
        _requirement(
            "source_2438k_runtime_remediation_ready",
            _source_2438k_ready(source_2438k),
            "source_2438k_not_ready",
            "source_2438k_runtime_remediation_gap",
            {
                "status": source_2438k.get("status"),
                "next_route": source_2438k.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "runtime_blocker_count_after_zero",
            _int(source_2438k.get("runtime_blocker_count_after"), -1) == 0,
            "runtime_blockers_remain_after_2438k",
            "runtime_remediation_gap",
            {"runtime_blocker_count_after": source_2438k.get("runtime_blocker_count_after")},
        ),
        _requirement(
            "candidate_replay_runtime_executable_count_three",
            _int(source_2438k.get("candidate_replay_runtime_executable_count"), 0) == 3,
            "candidate_runtime_executable_count_not_three",
            "runtime_remediation_gap",
            {
                "candidate_replay_runtime_executable_count": source_2438k.get(
                    "candidate_replay_runtime_executable_count"
                )
            },
        ),
        _requirement(
            "executable_replay_readiness_handoff_ready",
            _handoff_ready(handoff),
            "executable_replay_handoff_not_ready",
            "runtime_recheck_handoff_gap",
            {
                "executable_replay_readiness_handoff_ready": handoff.get(
                    "executable_replay_readiness_handoff_ready"
                )
            },
        ),
        _requirement(
            "handoff_candidates_match_runtime_records",
            runtime_records_ready and set(runtime_ids) == handoff_ids,
            "handoff_candidates_do_not_match_runtime_records",
            "runtime_recheck_handoff_gap",
            {"runtime_ids": runtime_ids, "handoff_ids": sorted(handoff_ids)},
        ),
        _requirement(
            "runtime_remediation_records_complete",
            runtime_records_ready,
            "runtime_remediation_records_incomplete",
            "runtime_remediation_record_gap",
            {"runtime_ids": runtime_ids, "runtime_record_count": len(runtime_records)},
        ),
        _requirement(
            "candidate_replay_output_records_complete",
            record_count_ready
            and _candidate_outputs_complete(candidate_replay_output_records, source_2438k)
            and set(candidate_ids) == set(runtime_ids),
            "candidate_output_records_incomplete",
            "candidate_replay_output_record_gap",
            {"candidate_ids": candidate_ids, "candidate_record_count": len(candidate_records)},
        ),
        _requirement(
            "candidate_runtime_decisions_complete",
            len(decisions) == 3
            and len(set(decision_ids)) == 3
            and set(decision_ids) == set(runtime_ids),
            "candidate_runtime_decisions_incomplete",
            "candidate_runtime_decision_gap",
            {"decision_ids": decision_ids, "runtime_ids": runtime_ids},
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_gap",
            None,
            {"data_quality_status": data_quality_summary.get("data_quality_status")},
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


def _candidate_decision(
    candidate_id: str,
    runtime_record: Mapping[str, Any],
    candidate_record: Mapping[str, Any],
    handoff_candidate: Mapping[str, Any],
    audit_record: Mapping[str, Any],
    as_of: str,
) -> dict[str, Any]:
    metric_summary = _metric_summary(runtime_record)
    threshold_evaluation = _threshold_evaluation(runtime_record)
    threshold_status = _threshold_status(threshold_evaluation)
    metric_values_ready = _metric_values_materialized(metric_summary)
    runtime_executable = (
        runtime_record.get("runtime_remediation_ready") is True
        and runtime_record.get("runtime_execution_smoke_check_status") == "PASS"
        and handoff_candidate.get("candidate_id") == candidate_id
    )
    baseline_ready = bool(
        runtime_record.get("baseline_comparison_materialized") is True
        and _first_present(
            runtime_record,
            candidate_record,
            "baseline_comparison_ref",
            "baseline_id",
        )
    )
    threshold_available = threshold_status in {"PASS", "FAIL", "BLOCKED"}
    evidence_ref = _first_present(runtime_record, candidate_record, "evidence_ref")
    source_traceability_ref = _first_present(
        runtime_record,
        candidate_record,
        "source_traceability_runtime_ref",
        "source_traceability_ref",
    )
    valid_until_ref = _first_present(
        runtime_record,
        candidate_record,
        "valid_until_policy_ref",
    )
    outcome_key = _first_present(runtime_record, candidate_record, "outcome_linkage_key")
    handoff_key = _first_present(
        runtime_record,
        candidate_record,
        handoff_candidate,
        "forward_aging_handoff_key",
    )
    blocker = _candidate_blocker(
        runtime_executable=runtime_executable,
        metric_values_ready=metric_values_ready,
        baseline_ready=baseline_ready,
        threshold_available=threshold_available,
        evidence_ref=evidence_ref,
        source_traceability_ref=source_traceability_ref,
        as_of=as_of,
        valid_until_ref=valid_until_ref,
        outcome_key=outcome_key,
        handoff_key=handoff_key,
        threshold_status=threshold_status,
        threshold_evaluation=threshold_evaluation,
    )
    if blocker:
        replay_status = "BLOCKED"
    elif threshold_status == "PASS":
        replay_status = "PASS"
    else:
        replay_status = "FAIL"
    status_reason = _status_reason(
        replay_status,
        threshold_evaluation,
        blocker,
        candidate_id,
    )
    failed_criteria = _sequence(threshold_evaluation.get("failed_criteria"))
    if replay_status == "FAIL" and not failed_criteria:
        failed_criteria = ("explicit_threshold_evaluation_fail",)
    return {
        "candidate_id": candidate_id,
        "replay_status": replay_status,
        "runtime_executable": runtime_executable,
        "runtime_execution_ref": _first_present(
            runtime_record,
            audit_record,
            handoff_candidate,
            "runtime_execution_smoke_check_ref",
        ),
        "metric_summary_ref": _first_present(
            runtime_record,
            "metric_materialization_ref",
        ),
        "baseline_comparison_ref": _first_present(
            runtime_record,
            candidate_record,
            "baseline_comparison_ref",
            "baseline_id",
        ),
        "threshold_evaluation_ref": _first_present(
            runtime_record,
            candidate_record,
            threshold_evaluation,
            "threshold_evaluation_ref",
            "threshold_evaluator_ref",
        ),
        "evidence_ref": evidence_ref,
        "source_traceability_ref": source_traceability_ref,
        "as_of_boundary_ref": as_of,
        "valid_until_policy_ref": valid_until_ref,
        "outcome_linkage_key": outcome_key,
        "return_delta_vs_baseline": metric_summary.get("return_delta_vs_baseline"),
        "max_drawdown_delta_vs_baseline": metric_summary.get(
            "max_drawdown_delta_vs_baseline"
        ),
        "turnover_delta_vs_baseline": metric_summary.get("turnover_delta_vs_baseline"),
        "false_risk_off_delta": metric_summary.get("false_risk_off_delta"),
        "missed_upside_delta": metric_summary.get("missed_upside_delta"),
        "whipsaw_delta": metric_summary.get("whipsaw_delta"),
        "metric_summary": metric_summary,
        "metric_values_materialized": metric_values_ready,
        "baseline_comparison_materialized": baseline_ready,
        "threshold_evaluation_available": threshold_available,
        "threshold_evaluation_status": threshold_status or None,
        "pass_reason": status_reason["pass_reason"],
        "fail_reason": status_reason["fail_reason"],
        "failed_criteria": list(failed_criteria) if replay_status == "FAIL" else [],
        "blocker_reason": status_reason["blocker_reason"],
        "remaining_blocker_category": blocker.get("blocker_category") if blocker else None,
        "required_next_action": blocker.get("required_next_action") if blocker else None,
        "forward_aging_handoff_key": handoff_key if replay_status == "PASS" else None,
        "eligible_for_forward_aging": replay_status == "PASS",
        "paper_shadow_candidate_found": False,
        "trading_advice_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_blocker(
    *,
    runtime_executable: bool,
    metric_values_ready: bool,
    baseline_ready: bool,
    threshold_available: bool,
    evidence_ref: object,
    source_traceability_ref: object,
    as_of: str,
    valid_until_ref: object,
    outcome_key: object,
    handoff_key: object,
    threshold_status: str,
    threshold_evaluation: Mapping[str, Any],
) -> dict[str, str] | None:
    checks: tuple[tuple[bool, str, str, str], ...] = (
        (
            runtime_executable,
            "runtime_not_executable_after_remediation",
            "Candidate runtime execution handoff is not executable after 2438K.",
            "Repair runtime remediation handoff before replay outcome recheck.",
        ),
        (
            metric_values_ready,
            "runtime_metric_values_not_materialized",
            "Runtime metric values remain missing or null after 2438K.",
            "Materialize numeric candidate replay metrics before PASS/FAIL evaluation.",
        ),
        (
            baseline_ready,
            "baseline_comparison_runtime_output_missing",
            "Baseline comparison runtime output is missing.",
            "Materialize baseline comparison before threshold evaluation.",
        ),
        (
            threshold_available,
            "threshold_evaluation_runtime_output_missing",
            "Explicit pass/fail threshold evaluation output is missing.",
            "Run or attach threshold evaluation before assigning PASS or FAIL.",
        ),
        (
            bool(evidence_ref),
            "candidate_replay_evidence_missing",
            "Candidate replay evidence reference is missing.",
            "Attach candidate replay evidence before outcome classification.",
        ),
        (
            bool(source_traceability_ref),
            "source_traceability_runtime_binding_missing",
            "Source traceability binding is missing.",
            "Bind source traceability before replay outcome classification.",
        ),
        (
            bool(as_of),
            "as_of_boundary_runtime_binding_missing",
            "As-of boundary is missing.",
            "Bind as-of boundary before replay outcome classification.",
        ),
        (
            bool(valid_until_ref),
            "valid_until_policy_runtime_binding_missing",
            "Valid-until policy binding is missing.",
            "Bind valid-until policy before replay outcome classification.",
        ),
        (
            bool(outcome_key),
            "outcome_linkage_key_runtime_binding_missing",
            "Outcome linkage key is missing.",
            "Bind outcome linkage before replay outcome classification.",
        ),
        (
            bool(handoff_key),
            "forward_aging_handoff_key_runtime_binding_missing",
            "Forward-aging handoff key is missing.",
            "Bind forward-aging handoff key before route evaluation.",
        ),
        (
            threshold_status != "BLOCKED",
            "threshold_evaluation_blocked",
            str(
                threshold_evaluation.get("blocker_reason")
                or "Threshold evaluation returned BLOCKED."
            ),
            "Resolve threshold-evaluation blocker before PASS/FAIL classification.",
        ),
    )
    for passed, category, reason, action in checks:
        if not passed:
            return {
                "blocker_category": category,
                "blocker_reason": reason,
                "required_next_action": action,
            }
    return None


def _status_reason(
    replay_status: str,
    threshold_evaluation: Mapping[str, Any],
    blocker: Mapping[str, Any] | None,
    candidate_id: str,
) -> dict[str, str | None]:
    if replay_status == "PASS":
        return {
            "pass_reason": str(
                threshold_evaluation.get("pass_reason")
                or f"{candidate_id} passed explicit runtime threshold evaluation."
            ),
            "fail_reason": None,
            "blocker_reason": None,
        }
    if replay_status == "FAIL":
        return {
            "pass_reason": None,
            "fail_reason": str(
                threshold_evaluation.get("fail_reason")
                or f"{candidate_id} failed explicit runtime threshold evaluation."
            ),
            "blocker_reason": None,
        }
    return {
        "pass_reason": None,
        "fail_reason": None,
        "blocker_reason": str(_mapping(blocker).get("blocker_reason") or ""),
    }


def _runtime_remediation_after_recheck(
    status: str,
    source_2438k: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    outcome_rechecked: bool,
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_AFTER_RECHECK_SCHEMA_VERSION,
        "status": status,
        "prior_status": source_2438k.get("status"),
        "runtime_remediation_ready": _source_2438k_ready(source_2438k),
        "runtime_blocker_count_after": source_2438k.get("runtime_blocker_count_after"),
        "candidate_replay_runtime_executable_count": source_2438k.get(
            "candidate_replay_runtime_executable_count"
        ),
        "candidate_replay_outcome_rechecked": outcome_rechecked,
        "candidate_runtime_decisions": list(decisions),
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _decision_matrix(
    status: str,
    decisions: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": DECISION_MATRIX_SCHEMA_VERSION,
        "status": status,
        "candidate_pass_fail_blocked_decision_matrix_ready": True,
        "rows": list(decisions),
        "decisions": list(decisions),
        "candidate_replay_pass_count": len(_decisions_with_status(decisions, "PASS")),
        "candidate_replay_fail_count": len(_decisions_with_status(decisions, "FAIL")),
        "candidate_replay_blocked_count": len(
            _decisions_with_status(decisions, "BLOCKED")
        ),
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _forward_aging_handoff_summary(
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
        "forward_aging_candidates": [
            {
                "candidate_id": candidate.get("candidate_id"),
                "forward_aging_handoff_key": candidate.get(
                    "forward_aging_handoff_key"
                ),
                "replay_status": candidate.get("replay_status"),
                "eligible_for_forward_aging": True,
            }
            for candidate in forward_aging_candidates
        ],
        "blocked_candidate_count": len(blocked_candidates),
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _post_runtime_blocker_summary(
    status: str,
    blockers: Sequence[Mapping[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": POST_RUNTIME_BLOCKER_SCHEMA_VERSION,
        "status": status,
        "post_runtime_candidate_replay_blocker_summary_ready": True,
        "post_runtime_candidate_replay_blocker_count": len(blockers),
        "post_runtime_candidate_replay_blockers": list(blockers),
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_effect_boundary(
    status: str,
    blockers: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "post_runtime_candidate_replay_blocker_count": len(blockers),
        "evidence_gap_count": len(gaps),
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


def _post_runtime_blockers(
    blocked_candidates: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    blockers = [
        {
            "candidate_id": candidate.get("candidate_id"),
            "blocker_category": candidate.get("remaining_blocker_category"),
            "blocker_reason": candidate.get("blocker_reason"),
            "required_next_action": candidate.get("required_next_action"),
            "production_effect": "none",
            "broker_action": "none",
        }
        for candidate in blocked_candidates
    ]
    if not blockers and gaps:
        blockers.extend(
            {
                "candidate_id": None,
                "blocker_category": gap.get("requirement_id"),
                "blocker_reason": gap.get("reason"),
                "required_next_action": "Resolve source requirement before replay recheck.",
                "production_effect": "none",
                "broker_action": "none",
            }
            for gap in gaps
        )
    return blockers


def _status_from_decisions(
    source_recheckable: bool,
    passing_candidates: Sequence[Mapping[str, Any]],
    failed_candidates: Sequence[Mapping[str, Any]],
    blocked_candidates: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
) -> str:
    if not source_recheckable or blocked_candidates:
        return BLOCKED_STATUS
    if passing_candidates:
        return READY_STATUS
    if len(failed_candidates) == 3 and len(decisions) == 3:
        return NO_PASSING_CANDIDATE_STATUS
    return BLOCKED_STATUS


def _next_route(status: str) -> str:
    if status == READY_STATUS:
        return NEXT_ROUTE_READY
    if status == NO_PASSING_CANDIDATE_STATUS:
        return NEXT_ROUTE_NO_PASS
    return NEXT_ROUTE_BLOCKED


def _runtime_records(
    source_2438k: Mapping[str, Any],
    runtime_materialization: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (runtime_materialization, source_2438k):
        section = _mapping(source.get("runtime_materialization_remediation"))
        records = _sequence(section.get("candidate_runtime_remediation_records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("candidate_runtime_remediation_records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _candidate_records(
    candidate_replay_output_records: Mapping[str, Any],
    source_2438k: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    for source in (candidate_replay_output_records, source_2438k):
        section = _mapping(source.get("candidate_replay_output_records"))
        records = _sequence(section.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
        records = _sequence(source.get("records"))
        if records:
            return [record for record in records if isinstance(record, Mapping)]
    return []


def _handoff_section(
    executable_handoff: Mapping[str, Any],
    source_2438k: Mapping[str, Any],
) -> Mapping[str, Any]:
    for source in (executable_handoff, source_2438k):
        section = _mapping(source.get("executable_replay_readiness_handoff"))
        if section:
            return section
    return {}


def _audit_section(
    runtime_audit: Mapping[str, Any],
    source_2438k: Mapping[str, Any],
) -> Mapping[str, Any]:
    for source in (runtime_audit, source_2438k):
        section = _mapping(source.get("runtime_execution_audit_trail"))
        if section:
            return section
    return {}


def _handoff_candidates(handoff: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        candidate
        for candidate in _sequence(handoff.get("handoff_candidates"))
        if isinstance(candidate, Mapping)
    ]


def _audit_records(audit: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        record
        for record in _sequence(audit.get("audit_records"))
        if isinstance(record, Mapping)
    ]


def _by_candidate_id(records: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {
        str(record.get("candidate_id")): record
        for record in records
        if record.get("candidate_id")
    }


def _decisions_with_status(
    decisions: Sequence[Mapping[str, Any]],
    replay_status: str,
) -> list[dict[str, Any]]:
    return [
        dict(decision)
        for decision in decisions
        if str(decision.get("replay_status", "")).upper() == replay_status
    ]


def _source_2438k_ready(source_2438k: Mapping[str, Any]) -> bool:
    next_route = source_2438k.get("recommended_next_research_task") or source_2438k.get(
        "next_route"
    )
    return (
        source_2438k.get("status") == EXPECTED_2438K_STATUS
        and next_route == EXPECTED_2438K_ROUTE
        and source_2438k.get("root_cause_remediation_ready") is True
        and source_2438k.get("replay_runtime_materialization_ready") is True
    )


def _handoff_ready(handoff: Mapping[str, Any]) -> bool:
    return (
        handoff.get("status") == EXPECTED_2438K_STATUS
        and handoff.get("executable_replay_readiness_handoff_ready") is True
        and handoff.get("ready_for_2438l_recheck") is True
        and _int(handoff.get("handoff_candidate_count"), 0) == 3
    )


def _candidate_outputs_complete(
    candidate_replay_output_records: Mapping[str, Any],
    source_2438k: Mapping[str, Any],
) -> bool:
    section = _mapping(candidate_replay_output_records.get("candidate_replay_output_records"))
    return (
        section.get("candidate_replay_output_records_ready") is True
        or source_2438k.get("candidate_replay_outputs_complete") is True
    )


def _metric_summary(runtime_record: Mapping[str, Any]) -> dict[str, Any]:
    runtime_summary = _mapping(runtime_record.get("metric_summary"))
    return {key: runtime_summary.get(key) for key in METRIC_KEYS}


def _metric_values_materialized(metric_summary: Mapping[str, Any]) -> bool:
    return all(_is_number(metric_summary.get(key)) for key in METRIC_KEYS)


def _threshold_evaluation(runtime_record: Mapping[str, Any]) -> Mapping[str, Any]:
    section = _mapping(runtime_record.get("threshold_evaluation"))
    if section:
        return section
    status = runtime_record.get("threshold_evaluation_status") or runtime_record.get(
        "replay_status_after_runtime_remediation"
    )
    if status:
        return {
            "status": status,
            "pass_reason": runtime_record.get("pass_reason"),
            "fail_reason": runtime_record.get("fail_reason"),
            "blocker_reason": runtime_record.get("blocker_reason"),
            "failed_criteria": runtime_record.get("failed_criteria", []),
            "threshold_evaluation_ref": runtime_record.get(
                "threshold_evaluation_ref",
                runtime_record.get("threshold_evaluator_ref"),
            ),
        }
    return {}


def _threshold_status(threshold_evaluation: Mapping[str, Any]) -> str:
    status = str(threshold_evaluation.get("status") or "").upper()
    return status if status in REPLAY_STATUSES else ""


def _first_present(*sources_and_keys: object) -> Any:
    sources: list[Mapping[str, Any]] = []
    keys: list[str] = []
    for item in sources_and_keys:
        if isinstance(item, Mapping):
            sources.append(item)
        elif isinstance(item, str):
            keys.append(item)
    for source in sources:
        for key in keys:
            value = source.get(key)
            if value not in (None, ""):
                return value
    return None


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", "")).upper()
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _candidate_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
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


def _docs_cover_runtime_recheck(text: str) -> bool:
    lowered = text.lower()
    return (
        "2438l" in lowered
        and "runtime" in lowered
        and "pass" in lowered
        and "blocked" in lowered
        and "forward-aging" in lowered
    )


def _next_route_reason(status: str) -> str:
    if status == READY_STATUS:
        return "At least one candidate passed runtime-remediated PIT replay recheck."
    if status == NO_PASSING_CANDIDATE_STATUS:
        return "All three candidates explicitly failed PIT replay recheck with no blockers."
    return "At least one candidate or source remains blocked after runtime remediation."


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


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
