from __future__ import annotations

from collections.abc import Mapping
from typing import Any

CLOSURE_SCHEMA_VERSION = (
    "growth_tilt_engine_valid_until_dependency_evidence_closure.v1"
)
VALID_UNTIL_DEPENDENCY_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_engine_valid_until_dependency_evidence.v1"
)
SIGNAL_VALIDITY_CONTRACT_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_validity_contract_evidence.v1"
)
STALE_SIGNAL_POLICY_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_engine_stale_signal_policy_evidence.v1"
)
GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_engine_valid_until_alignment_evidence.v1"
)
REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_remaining_blocker_summary_after_valid_until_closure.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY"
NEXT_ROUTE = "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck"
FOLLOWUP_ROUTE = "TRADING-2418A_Valid_Until_Window_Dependency_Evidence_Followup"

TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
DEPENDENCY_FEATURE_ID = "execution_signal_validity_policy"
SOURCE_TRACEABILITY_STILL_BLOCKED_FEATURE = "growth_tilt_engine_signal_artifact"

SIGNAL_VALIDITY_REQUIRED_FIELDS: tuple[str, ...] = (
    "signal_id",
    "signal_version",
    "as_of_date",
    "generated_at",
    "source_data_cutoff",
    "valid_from",
    "valid_until",
    "stale_after",
    "horizon_days",
    "expiry_rule",
    "carry_forward_rule",
    "near_expiry_rule",
    "signal_to_execution_lag_rule",
)


def build_growth_tilt_valid_until_dependency_evidence_closure(
    closure_result_2417: Mapping[str, Any],
    remaining_blocker_summary_2417: Mapping[str, Any],
    closure_result_2416: Mapping[str, Any],
    remaining_blocker_matrix_2416: Mapping[str, Any],
    valid_until_dependency_closure_plan_2416: Mapping[str, Any],
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    pit_gate_readiness_matrix_2415: Mapping[str, Any],
    signal_validity_dependency_remediation_result_2414: Mapping[str, Any],
    signal_validity_dependency_contract_metadata_2414: Mapping[str, Any],
    remaining_blocker_summary_2414: Mapping[str, Any],
    remediation_plan_result_2411: Mapping[str, Any],
    remediation_plan_result_2407: Mapping[str, Any],
    valid_until_semantics_review_2407: Mapping[str, Any],
    stale_signal_risk_audit_2407: Mapping[str, Any],
    signal_validity_contract_plan_2407: Mapping[str, Any],
    validation_plan_2407: Mapping[str, Any],
    *,
    pit_input_registry: Mapping[str, Any] | None = None,
    strategy_execution_policy_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    blocker_rows = [
        _as_mapping(row)
        for row in _as_list(
            _section(remaining_blocker_matrix_2416, "remaining_blocker_matrix").get(
                "matrix_rows"
            )
        )
    ]
    dependency_row = _row_by_feature(blocker_rows, DEPENDENCY_FEATURE_ID)
    metadata_rows = [
        _as_mapping(row)
        for row in _as_list(
            _section(
                signal_validity_dependency_contract_metadata_2414,
                "signal_validity_dependency_contract_metadata",
            ).get("metadata_rows")
        )
    ]
    metadata_row = _row_by_feature(metadata_rows, DEPENDENCY_FEATURE_ID)
    signal_artifact_still_blocked = _source_traceability_still_blocked(
        remaining_blocker_summary_2417
    )
    strategy_entry = _strategy_entry(
        strategy_execution_policy_registry or {},
        TARGET_STRATEGY_ID,
    )
    signal_policy = _as_mapping(strategy_entry.get("signal_policy"))
    rebalance_policy = _as_mapping(strategy_entry.get("rebalance_policy"))
    contract_plan = _section(
        signal_validity_contract_plan_2407,
        "signal_validity_contract_plan",
    )
    semantics = _section(
        valid_until_semantics_review_2407,
        "valid_until_semantics_review",
    )
    stale_audit = _section(stale_signal_risk_audit_2407, "stale_signal_risk_audit")
    validation_plan = _section(validation_plan_2407, "validation_plan")
    alignment_review = _as_mapping(
        _as_mapping(remediation_plan_result_2407).get("growth_tilt_alignment_review")
    )
    valid_until_plan = _section(
        valid_until_dependency_closure_plan_2416,
        "valid_until_dependency_closure_plan",
    )
    counts = _counts(
        closure_result_2417,
        closure_result_2416,
        pit_gate_readiness_snapshot_2415,
        blocker_rows,
    )
    valid_until_evidence = _valid_until_dependency_evidence(
        dependency_row,
        metadata_row,
        valid_until_plan,
        contract_plan,
        semantics,
        stale_audit,
        signal_policy,
        rebalance_policy,
    )
    contract_evidence = _signal_validity_contract_evidence(
        contract_plan,
        semantics,
        signal_policy,
        rebalance_policy,
    )
    stale_policy_evidence = _stale_signal_policy_evidence(
        contract_plan,
        stale_audit,
        signal_policy,
        validation_plan,
    )
    alignment_evidence = _alignment_evidence(
        alignment_review,
        semantics,
        signal_artifact_still_blocked=signal_artifact_still_blocked,
    )
    remaining_summary = _remaining_blocker_summary(
        counts,
        valid_until_evidence=valid_until_evidence,
        signal_artifact_still_blocked=signal_artifact_still_blocked,
        pit_input_registry=pit_input_registry or {},
    )
    validation = _validation(
        counts,
        dependency_row=dependency_row,
        metadata_row=metadata_row,
        valid_until_plan=valid_until_plan,
        contract_plan=contract_plan,
        strategy_entry=strategy_entry,
        signal_artifact_still_blocked=signal_artifact_still_blocked,
        pit_gate_readiness_matrix_2415=pit_gate_readiness_matrix_2415,
        remediation_result_2414=signal_validity_dependency_remediation_result_2414,
        remaining_summary_2414=remaining_blocker_summary_2414,
        remediation_result_2411=remediation_plan_result_2411,
    )
    evidence_ready = validation["valid"]
    return {
        "schema_version": CLOSURE_SCHEMA_VERSION,
        "task_id": "TRADING-2418",
        "status": READY_STATUS,
        "engine_id": "growth_tilt_engine",
        "source_tasks": [
            "TRADING-2407",
            "TRADING-2411",
            "TRADING-2414",
            "TRADING-2415",
            "TRADING-2416",
            "TRADING-2417",
        ],
        "target_strategy_id": TARGET_STRATEGY_ID,
        "source_feature_count": counts["source_feature_count"],
        "pit_gate_ready_count": counts["pit_gate_ready_count"],
        "contract_ready_count": counts["contract_ready_count"],
        "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
        "blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "valid_until_window_dependency_blocker_count_from_2415": counts[
            "blocked_by_valid_until_window_count"
        ],
        "valid_until_dependency_evidence_row_count": len(
            valid_until_evidence["evidence_rows"]
        ),
        "valid_until_dependency_pre_recheck_evidence_ready_count": (
            1 if evidence_ready else 0
        ),
        "valid_until_dependency_still_blocked_count": 0 if evidence_ready else 1,
        "valid_until_dependency_evidence_ready": evidence_ready,
        "signal_validity_contract_evidence_ready": evidence_ready,
        "stale_signal_policy_evidence_ready": evidence_ready,
        "growth_tilt_valid_until_alignment_evidence_ready": evidence_ready,
        "remaining_blocker_summary_ready": evidence_ready,
        "source_traceability_still_blocked": (
            [SOURCE_TRACEABILITY_STILL_BLOCKED_FEATURE]
            if signal_artifact_still_blocked
            else []
        ),
        "valid_until_dependency_evidence": valid_until_evidence,
        "signal_validity_contract_evidence": contract_evidence,
        "stale_signal_policy_evidence": stale_policy_evidence,
        "growth_tilt_valid_until_alignment_evidence": alignment_evidence,
        "remaining_blocker_summary": remaining_summary,
        "closure_validation": validation,
        "pit_gate_recheck_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "auto_downgrade_blocker": False,
        "owner_review_required_before_downgrade": True,
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2417 supplied source traceability pre-recheck evidence and "
            "TRADING-2418 supplies valid_until dependency pre-recheck evidence; "
            "the next step is a separate PIT gate readiness recheck without "
            "automatic blocker downgrade."
        ),
    }


def _valid_until_dependency_evidence(
    dependency_row: Mapping[str, Any],
    metadata_row: Mapping[str, Any],
    valid_until_plan: Mapping[str, Any],
    contract_plan: Mapping[str, Any],
    semantics: Mapping[str, Any],
    stale_audit: Mapping[str, Any],
    signal_policy: Mapping[str, Any],
    rebalance_policy: Mapping[str, Any],
) -> dict[str, Any]:
    semantic_by_id = _semantics_by_id(semantics)
    primary_semantic = _as_mapping(semantic_by_id.get("valid_until_window"))
    lag_semantic = _as_mapping(semantic_by_id.get("signal_to_execution_lag"))
    growth_alignment = _as_mapping(semantic_by_id.get("growth_tilt_valid_until_alignment"))
    decision_policy = _as_mapping(contract_plan.get("decision_policy"))
    window_bdays = signal_policy.get("signal_validity_window_bdays")
    lag_bdays = rebalance_policy.get("execution_lag_bdays")
    evidence_row = {
        "dependency_id": _text(
            metadata_row.get("validity_dependency_id")
            or "growth_tilt_engine:execution_signal_validity_policy:signal_validity_dependency:v1"
        ),
        "dependent_feature_or_signal": DEPENDENCY_FEATURE_ID,
        "dependency_type": [
            "SIGNAL_VALIDITY_CONTRACT",
            "VALID_FROM_MAPPING",
            "VALID_UNTIL_MAPPING",
            "STALE_AFTER_MAPPING",
            "EXPIRY_RULE",
            "CARRY_FORWARD_RULE",
            "SIGNAL_TO_EXECUTION_LAG",
            "NEAR_EXPIRY_POLICY",
            "GROWTH_TILT_HORIZON_ALIGNMENT",
        ],
        "source_system": dependency_row.get("source_system"),
        "source_reference": dependency_row.get("upstream_artifact_or_registry_reference"),
        "valid_from_source": (
            primary_semantic.get("valid_from_source")
            or signal_policy.get("signal_effective_earliest")
        ),
        "valid_until_source": (
            primary_semantic.get("valid_until_source")
            or f"signal_validity_window_bdays={window_bdays}"
        ),
        "stale_after_source": _stale_after_source(contract_plan, stale_audit),
        "expiry_rule_source": (
            primary_semantic.get("expiry_rule_source")
            or decision_policy.get("current_date > valid_until")
        ),
        "carry_forward_rule_source": (
            primary_semantic.get("carry_forward_rule")
            or signal_policy.get("stale_signal_behavior")
        ),
        "signal_to_execution_lag_source": (
            lag_semantic.get("signal_to_execution_lag_rule")
            or f"execution_lag_bdays={lag_bdays}"
        ),
        "near_expiry_rule_source": decision_policy.get("near valid_until"),
        "growth_tilt_horizon_alignment_source": (
            growth_alignment.get("signal_horizon_source")
            or "TRADING-2407 growth_tilt_alignment_review"
        ),
        "policy_window_bdays": window_bdays,
        "execution_lag_bdays": lag_bdays,
        "before_status_from_2416": {
            "blocked_by_valid_until_window": dependency_row.get(
                "blocked_by_valid_until_window"
            ),
            "valid_until_available": dependency_row.get("valid_until_available"),
            "valid_until_required": dependency_row.get("valid_until_required"),
            "validity_dependency_status": dependency_row.get(
                "validity_dependency_status"
            ),
            "current_pit_gate_status": dependency_row.get("current_pit_gate_status"),
            "required_closure_evidence": list(
                _as_list(dependency_row.get("required_closure_evidence"))
            ),
        },
        "contract_metadata_from_2414": {
            "valid_until_required": metadata_row.get("valid_until_required"),
            "valid_until_available": metadata_row.get("valid_until_available"),
            "validity_basis": metadata_row.get("validity_basis"),
            "validity_blocking_reason": metadata_row.get("validity_blocking_reason"),
            "validity_end_reference": metadata_row.get("validity_end_reference"),
            "staleness_policy": metadata_row.get("staleness_policy"),
            "expiration_policy": metadata_row.get("expiration_policy"),
        },
        "evidence_status": "CLOSED_WITH_EVIDENCE",
        "remaining_gap": (
            "evidence is ready for PIT gate recheck, but valid_until_window blocker "
            "and PIT/contract readiness remain unchanged until TRADING-2419 and "
            "owner review"
        ),
        "ready_for_pit_gate_recheck": True,
        "pit_gate_ready_after_2418": False,
        "contract_ready_after_2418": False,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {
        "schema_version": VALID_UNTIL_DEPENDENCY_EVIDENCE_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "evidence_scope": "pre_recheck_valid_until_dependency_evidence",
        "dependency_feature_id": DEPENDENCY_FEATURE_ID,
        "valid_until_window_dependency_blocker_count_from_2415": _int(
            valid_until_plan.get("dependent_feature_or_signal_count"), 1
        ),
        "dependent_feature_ids": list(
            _as_list(valid_until_plan.get("dependent_feature_ids"))
            or [DEPENDENCY_FEATURE_ID]
        ),
        "evidence_rows": [evidence_row],
        "pre_recheck_evidence_ready_count": 1,
        "still_blocked_count": 0,
        "pit_gate_recheck_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _signal_validity_contract_evidence(
    contract_plan: Mapping[str, Any],
    semantics: Mapping[str, Any],
    signal_policy: Mapping[str, Any],
    rebalance_policy: Mapping[str, Any],
) -> dict[str, Any]:
    template = _as_mapping(contract_plan.get("example_contract_template"))
    decision_policy = _as_mapping(contract_plan.get("decision_policy"))
    semantic_by_id = _semantics_by_id(semantics)
    primary_semantic = _as_mapping(semantic_by_id.get("valid_until_window"))
    field_sources = {
        "signal_id": template.get("signal_id") or "growth_tilt_engine",
        "signal_version": template.get("signal_version"),
        "as_of_date": template.get("as_of_date"),
        "generated_at": template.get("generated_at"),
        "source_data_cutoff": template.get("source_data_cutoff"),
        "valid_from": template.get("valid_from")
        or primary_semantic.get("valid_from_source"),
        "valid_until": template.get("valid_until")
        or primary_semantic.get("valid_until_source"),
        "stale_after": template.get("stale_after")
        or decision_policy.get("current_date > stale_after"),
        "horizon_days": template.get("horizon_days")
        or signal_policy.get("signal_validity_window_bdays"),
        "expiry_rule": template.get("expiry_rule")
        or decision_policy.get("current_date > valid_until"),
        "carry_forward_rule": signal_policy.get("stale_signal_behavior"),
        "near_expiry_rule": decision_policy.get("near valid_until"),
        "signal_to_execution_lag_rule": (
            f"execution_lag_bdays={rebalance_policy.get('execution_lag_bdays')}"
        ),
    }
    rows = [
        {
            "field": field,
            "evidence_available": bool(field_sources.get(field) is not None),
            "evidence_source": field_sources.get(field),
            "source_task": _field_source_task(field),
            "implementation_status": (
                "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY"
            ),
        }
        for field in SIGNAL_VALIDITY_REQUIRED_FIELDS
    ]
    available_count = sum(1 for row in rows if row["evidence_available"])
    return {
        "schema_version": SIGNAL_VALIDITY_CONTRACT_EVIDENCE_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "signal_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "required_field_count": len(SIGNAL_VALIDITY_REQUIRED_FIELDS),
        "evidence_available_count": available_count,
        "missing_field_count": len(SIGNAL_VALIDITY_REQUIRED_FIELDS)
        - available_count,
        "ready_for_recheck": available_count == len(SIGNAL_VALIDITY_REQUIRED_FIELDS),
        "required_fields": list(SIGNAL_VALIDITY_REQUIRED_FIELDS),
        "field_evidence_rows": rows,
        "source_policy_context": dict(
            _as_mapping(contract_plan.get("source_policy_context"))
        ),
        "invariants": list(_as_list(contract_plan.get("invariants"))),
        "decision_policy": dict(decision_policy),
        "pit_gate_recheck_required": True,
        "auto_mark_contract_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _stale_signal_policy_evidence(
    contract_plan: Mapping[str, Any],
    stale_audit: Mapping[str, Any],
    signal_policy: Mapping[str, Any],
    validation_plan: Mapping[str, Any],
) -> dict[str, Any]:
    decision_policy = _as_mapping(contract_plan.get("decision_policy"))
    risks = [
        _as_mapping(risk)
        for risk in _as_list(stale_audit.get("risks"))
        if _as_mapping(risk).get("category")
        in {
            "CARRY_FORWARD_RISK",
            "SIGNAL_TO_EXECUTION_LAG_RISK",
            "NEAR_EXPIRY_OVERTRADING_RISK",
            "SIGNAL_REFRESH_COLLISION_RISK",
        }
    ]
    invariants = {
        "expired_signal_cannot_trigger_new_trade": True,
        "missing_valid_until_blocks_dependent_strategy_recheck": True,
        "carry_forward_requires_explicit_rule": True,
        "owner_review_required_for_carry_forward_in_observation_context": True,
    }
    policy_row = {
        "policy_id": "growth_tilt_engine_valid_until_window_no_stale_policy_v1",
        "expired_signal_execution_rule": decision_policy.get(
            "current_date > valid_until"
        ),
        "carry_forward_rule": signal_policy.get("stale_signal_behavior"),
        "carry_forward_requires_owner_approval": True,
        "stale_after_rule": decision_policy.get("current_date > stale_after"),
        "near_expiry_rule": decision_policy.get("near valid_until"),
        "signal_refresh_collision_rule": decision_policy.get("new signal overlaps old"),
        "validation_plan_stale_replay": list(_as_list(validation_plan.get("stale_replay"))),
        "risk_rows": risks,
        "evidence_status": "CLOSED_WITH_EVIDENCE",
        "remaining_gap": (
            "replay validation and owner review are still required before any "
            "blocker downgrade or observation approval"
        ),
    }
    return {
        "schema_version": STALE_SIGNAL_POLICY_EVIDENCE_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "policy_evidence_rows": [policy_row],
        "required_policy_invariants": invariants,
        "stale_carry_forward_policy_ready": True,
        "signal_to_execution_lag_policy_ready": True,
        "deterministic_next_executable_time_policy_ready": True,
        "replay_validation_required": True,
        "ready_for_recheck": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _alignment_evidence(
    alignment_review: Mapping[str, Any],
    semantics: Mapping[str, Any],
    *,
    signal_artifact_still_blocked: bool,
) -> dict[str, Any]:
    semantic_by_id = _semantics_by_id(semantics)
    alignment_semantic = _as_mapping(semantic_by_id.get("growth_tilt_valid_until_alignment"))
    gap_summary = _as_mapping(alignment_review.get("alignment_gap_summary"))
    row = {
        "growth_tilt_signal_id": "growth_tilt_engine",
        "growth_tilt_signal_horizon_source": (
            gap_summary.get("growth_tilt_signal_horizon")
            or alignment_semantic.get("signal_horizon_source")
        ),
        "valid_until_window_source": alignment_semantic.get("valid_until_source"),
        "horizon_to_valid_until_mapping_available": bool(
            _as_list(alignment_review.get("proposed_horizon_to_valid_until_mapping"))
        ),
        "confidence_to_expiry_mapping_available": bool(
            _as_list(alignment_review.get("proposed_confidence_to_expiry_mapping"))
        ),
        "high_volatility_expiry_adjustment_available": (
            gap_summary.get("high_volatility_shrink_rule") not in {None, "missing"}
        ),
        "recovery_state_expiry_adjustment_available": (
            gap_summary.get("recovery_conservatism_rule") not in {None, "missing"}
        ),
        "alignment_questions": list(_as_list(alignment_review.get("alignment_questions"))),
        "alignment_status": (
            "BLOCKED_BY_SOURCE_SIGNAL_ARTIFACT"
            if signal_artifact_still_blocked
            else "PARTIALLY_ALIGNED"
        ),
        "remaining_gap": (
            "growth_tilt_engine_signal_artifact remains source-traceability blocked; "
            "PIT recheck must keep this blocker until signal artifact metadata exists"
            if signal_artifact_still_blocked
            else "owner calibration and replay validation required before downgrade"
        ),
        "ready_for_pit_gate_recheck": True,
        "pit_gate_ready_after_2418": False,
        "contract_ready_after_2418": False,
    }
    return {
        "schema_version": GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "alignment_rows": [row],
        "proposed_horizon_to_valid_until_mapping": list(
            _as_list(alignment_review.get("proposed_horizon_to_valid_until_mapping"))
        ),
        "proposed_confidence_to_expiry_mapping": list(
            _as_list(alignment_review.get("proposed_confidence_to_expiry_mapping"))
        ),
        "source_traceability_still_blocked": (
            [SOURCE_TRACEABILITY_STILL_BLOCKED_FEATURE]
            if signal_artifact_still_blocked
            else []
        ),
        "ready_for_recheck": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_blocker_summary(
    counts: Mapping[str, int],
    *,
    valid_until_evidence: Mapping[str, Any],
    signal_artifact_still_blocked: bool,
    pit_input_registry: Mapping[str, Any],
) -> dict[str, Any]:
    source_still_blocked = (
        [SOURCE_TRACEABILITY_STILL_BLOCKED_FEATURE]
        if signal_artifact_still_blocked
        else []
    )
    categories = [
        {
            "blocker_category": "SOURCE_TRACEABILITY_GAP",
            "before_count": len(source_still_blocked),
            "closure_evidence_added_count": 0,
            "still_blocked_count": len(source_still_blocked),
            "requires_pit_gate_recheck_count": len(source_still_blocked),
            "requires_owner_review_count": len(source_still_blocked),
            "known_still_blocked_feature_ids": source_still_blocked,
            "recommended_next_task": NEXT_ROUTE,
        },
        {
            "blocker_category": "VALID_UNTIL_DEPENDENCY_GAP",
            "before_count": counts["blocked_by_valid_until_window_count"],
            "closure_evidence_added_count": len(
                _as_list(valid_until_evidence.get("evidence_rows"))
            ),
            "still_blocked_count": 0,
            "requires_pit_gate_recheck_count": counts[
                "blocked_by_valid_until_window_count"
            ],
            "requires_owner_review_count": counts[
                "blocked_by_valid_until_window_count"
            ],
            "known_still_blocked_feature_ids": [],
            "recommended_next_task": NEXT_ROUTE,
        },
        {
            "blocker_category": "PIT_GATE_EVIDENCE_GAP",
            "before_count": counts["pit_gate_blocked_count"],
            "closure_evidence_added_count": 1,
            "still_blocked_count": counts["pit_gate_blocked_count"],
            "requires_pit_gate_recheck_count": counts["pit_gate_blocked_count"],
            "requires_owner_review_count": counts["pit_gate_blocked_count"],
            "known_still_blocked_feature_ids": [],
            "recommended_next_task": NEXT_ROUTE,
        },
        {
            "blocker_category": "OWNER_REVIEW_GAP",
            "before_count": 2,
            "closure_evidence_added_count": 0,
            "still_blocked_count": 2,
            "requires_pit_gate_recheck_count": 2,
            "requires_owner_review_count": 2,
            "known_still_blocked_feature_ids": [
                "growth_tilt_engine",
                "valid_until_window",
            ],
            "recommended_next_task": NEXT_ROUTE,
        },
    ]
    return {
        "schema_version": REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_feature_count": counts["source_feature_count"],
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
        "blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "source_traceability_still_blocked_feature_ids": source_still_blocked,
        "valid_until_window_dependency_evidence_added_feature_ids": [
            DEPENDENCY_FEATURE_ID
        ],
        "blocker_category_rows": categories,
        "growth_tilt_engine_pit_input_severity": _pit_registry_severity(
            pit_input_registry,
            "growth_tilt_engine",
        ),
        "valid_until_window_pit_input_severity": _pit_registry_severity(
            pit_input_registry,
            "valid_until_window",
        ),
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "pit_gate_recheck_required": True,
        "recommended_next_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _validation(
    counts: Mapping[str, int],
    *,
    dependency_row: Mapping[str, Any],
    metadata_row: Mapping[str, Any],
    valid_until_plan: Mapping[str, Any],
    contract_plan: Mapping[str, Any],
    strategy_entry: Mapping[str, Any],
    signal_artifact_still_blocked: bool,
    pit_gate_readiness_matrix_2415: Mapping[str, Any],
    remediation_result_2414: Mapping[str, Any],
    remaining_summary_2414: Mapping[str, Any],
    remediation_result_2411: Mapping[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    expected_counts = {
        "source_feature_count": 10,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 10,
        "blocked_by_valid_until_window_count": 1,
    }
    for field, expected in expected_counts.items():
        if counts.get(field) != expected:
            errors.append(f"{field} must remain {expected}")
    if dependency_row.get("blocked_by_valid_until_window") is not True:
        errors.append("execution_signal_validity_policy must remain valid_until blocked")
    if dependency_row.get("valid_until_required") is not True:
        errors.append("execution_signal_validity_policy must require valid_until")
    if metadata_row.get("valid_until_required") is not True:
        errors.append("2414 metadata must require valid_until for dependency feature")
    if metadata_row.get("validity_blocking_reason") != "valid_until_window_unresolved":
        errors.append("2414 metadata must keep valid_until_window_unresolved")
    if DEPENDENCY_FEATURE_ID not in _as_list(valid_until_plan.get("dependent_feature_ids")):
        errors.append("2416 valid-until plan must target execution_signal_validity_policy")
    if not _as_mapping(contract_plan.get("decision_policy")):
        errors.append("2407 contract plan must expose decision_policy")
    if not _as_mapping(contract_plan.get("example_contract_template")):
        errors.append("2407 contract plan must expose example_contract_template")
    if _as_mapping(strategy_entry.get("signal_policy")).get(
        "signal_validity_window_bdays"
    ) is None:
        errors.append("strategy execution policy registry must expose validity window")
    if _as_mapping(strategy_entry.get("rebalance_policy")).get("execution_lag_bdays") is None:
        errors.append("strategy execution policy registry must expose execution lag")
    if not signal_artifact_still_blocked:
        errors.append("2417 must preserve growth_tilt_engine_signal_artifact blocker")
    readiness_rows = _as_list(
        _section(pit_gate_readiness_matrix_2415, "pit_gate_readiness_matrix").get(
            "matrix_rows"
        )
    )
    if len(readiness_rows) != 10:
        errors.append("2415 readiness matrix must contain 10 rows")
    if remediation_result_2414.get(
        "validity_dependency_blocked_by_valid_until_window_count"
    ) != 1:
        errors.append("2414 must keep one valid_until dependency blocker")
    summary_2414 = _section(remaining_summary_2414, "remaining_blocker_summary")
    if summary_2414.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2414 summary must not resolve valid_until_window blocker")
    if remediation_result_2411.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2411 must not resolve valid_until_window blocker")
    return {
        "schema_version": (
            "growth_tilt_engine_valid_until_dependency_evidence_closure_validation.v1"
        ),
        "valid": not errors,
        "error_count": len(errors),
        "errors": errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def _counts(
    closure_result_2417: Mapping[str, Any],
    closure_result_2416: Mapping[str, Any],
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    blocker_rows: list[Mapping[str, Any]],
) -> dict[str, int]:
    return {
        "source_feature_count": _int(
            closure_result_2417.get(
                "source_feature_count",
                closure_result_2416.get(
                    "source_feature_count",
                    pit_gate_readiness_snapshot_2415.get(
                        "source_feature_count",
                        len(blocker_rows),
                    ),
                ),
            )
        ),
        "pit_gate_ready_count": _int(closure_result_2417.get("pit_gate_ready_count", 0)),
        "contract_ready_count": _int(closure_result_2417.get("contract_ready_count", 0)),
        "pit_gate_blocked_count": _int(
            closure_result_2417.get("pit_gate_blocked_count", len(blocker_rows))
        ),
        "blocked_by_source_traceability_count": _int(
            closure_result_2417.get("blocked_by_source_traceability_count", 0)
        ),
        "blocked_by_valid_until_window_count": _int(
            closure_result_2417.get(
                "blocked_by_valid_until_window_count",
                sum(
                    1
                    for row in blocker_rows
                    if row.get("blocked_by_valid_until_window") is True
                ),
            )
        ),
    }


def _source_traceability_still_blocked(
    remaining_blocker_summary_2417: Mapping[str, Any],
) -> bool:
    summary = _section(remaining_blocker_summary_2417, "remaining_blocker_summary")
    return SOURCE_TRACEABILITY_STILL_BLOCKED_FEATURE in _as_list(
        summary.get("source_traceability_still_blocked_feature_ids")
    )


def _stale_after_source(
    contract_plan: Mapping[str, Any],
    stale_audit: Mapping[str, Any],
) -> str | None:
    template = _as_mapping(contract_plan.get("example_contract_template"))
    if template.get("stale_after"):
        return _text(template.get("stale_after"))
    for risk in _as_list(stale_audit.get("risks")):
        mapping = _as_mapping(risk)
        if mapping.get("category") in {"VALID_UNTIL_UNGROUNDED", "CARRY_FORWARD_RISK"}:
            return _text(mapping.get("recommended_fix"))
    return None


def _field_source_task(field: str) -> str:
    if field in {
        "signal_id",
        "signal_version",
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "valid_from",
        "valid_until",
        "stale_after",
        "horizon_days",
        "expiry_rule",
    }:
        return "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    if field in {"carry_forward_rule", "near_expiry_rule"}:
        return "TRADING-2407_VALID_UNTIL_SEMANTICS_REVIEW"
    return "CONFIG_STRATEGY_EXECUTION_POLICY_REGISTRY"


def _semantics_by_id(semantics: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        _text(row.get("semantic_id")): row
        for row in (_as_mapping(item) for item in _as_list(semantics.get("semantics")))
        if _text(row.get("semantic_id"))
    }


def _strategy_entry(registry: Mapping[str, Any], strategy_id: str) -> Mapping[str, Any]:
    for entry in _as_list(registry.get("strategy_execution_policies")):
        mapping = _as_mapping(entry)
        if mapping.get("strategy_id") == strategy_id:
            return mapping
    return {}


def _row_by_feature(
    rows: list[Mapping[str, Any]],
    feature_id: str,
) -> Mapping[str, Any]:
    for row in rows:
        if _feature_id(row) == feature_id:
            return row
    return {}


def _feature_id(row: Mapping[str, Any]) -> str:
    return _text(
        row.get("feature_id")
        or row.get("source_feature_id")
        or row.get("source_feature_name")
    )


def _pit_registry_severity(registry: Mapping[str, Any], input_id: str) -> str | None:
    for entry in _as_list(registry.get("entries")):
        mapping = _as_mapping(entry)
        if mapping.get("input_id") == input_id:
            severity = mapping.get("severity")
            return severity if isinstance(severity, str) else None
    return None


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    mapping = _as_mapping(document)
    nested = mapping.get(key)
    return _as_mapping(nested) if isinstance(nested, Mapping) else mapping


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
