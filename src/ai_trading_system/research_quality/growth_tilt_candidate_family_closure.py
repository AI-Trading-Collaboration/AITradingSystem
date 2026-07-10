from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_candidate_family_closure.v1"
LEDGER_SCHEMA_VERSION = "growth_tilt_candidate_negative_result_ledger.v1"
REOPEN_SCHEMA_VERSION = "growth_tilt_candidate_family_reopen_policy.v1"

REPORT_TYPE = "growth_tilt_candidate_family_closure"
READY_STATUS = "GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE"
BLOCKED_STATUS = "GROWTH_TILT_CANDIDATE_FAMILY_CLOSURE_BLOCKED_SOURCE_CONTRACT"
CLOSURE_STATUS = "CLOSED_NO_EXECUTABLE_PIT_CANDIDATE"
NEXT_ROUTE = "TRADING-2438N2_GROWTH_TILT_BASELINE_CAPABILITY_GRAPH"
FAMILY_ID = "growth_tilt_false_risk_off_missed_upside_2433"

EXPECTED_M1E_SCHEMA = "growth_tilt_replacement_candidate_contract.v1"
EXPECTED_M1E_STATUS = "GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT_READY_BLOCKED"
EXPECTED_ADAPTER_SCHEMA = "growth_tilt_baseline_contract_adapters_readiness.v1"
EXPECTED_ADAPTER_STATUS = "GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_READY_WITH_BLOCKERS"
EXPECTED_OWNER_SCHEMA = "growth_tilt_owner_decision_resolution.v1"
EXPECTED_CANDIDATE_SET_SCHEMA = (
    "growth_tilt_false_risk_off_missed_upside_candidate_set.v1"
)
EXPECTED_PREREQUISITE_IDS = (
    "replacement_identity_and_orthogonal_role_frozen",
    "recovery_output_semantic_type_known",
    "recovery_producer_pit_lineage_valid",
    "recovery_threshold_versioned_and_preregistered",
    "hard_veto_aggregate_contract_ready",
    "current_requested_applied_transition_contract_ready",
    "native_exposure_scalar_contract_ready",
    "candidate_cap_expressed_in_native_units",
    "screening_policy_frozen_before_result_visibility",
    "second_owner_approval_recorded",
)
EXPECTED_BLOCKER_CODES = (
    "RECOVERY_PRODUCER_PIT_LINEAGE_VALID",
    "RECOVERY_THRESHOLD_VERSIONED_AND_PREREGISTERED",
    "HARD_VETO_AGGREGATE_CONTRACT_READY",
    "CURRENT_REQUESTED_APPLIED_TRANSITION_CONTRACT_READY",
    "NATIVE_EXPOSURE_SCALAR_CONTRACT_READY",
    "CANDIDATE_CAP_EXPRESSED_IN_NATIVE_UNITS",
    "SCREENING_POLICY_FROZEN_BEFORE_RESULT_VISIBILITY",
    "SECOND_OWNER_APPROVAL_RECORDED",
)
EXPECTED_DISPOSITIONS = {
    "recovery_reentry_speedup_guard": "REDEFINE",
    "false_risk_off_confirmation_relaxation": "WITHDRAW",
    "missed_upside_reentry_accelerator": "REDEFINE",
    "capped_recovery_permission_overlay": "KEEP_REDEFINED_BLOCKED",
}
CLOSURE_REASON_CODES = (
    "NO_APPROVED_CANDIDATE",
    "BASELINE_ADAPTERS_NOT_RUNTIME_READY",
    "REPLACEMENT_CANDIDATE_PREREQUISITES_BLOCKED",
    "NO_REAL_PIT_REPLAY_EXECUTED",
)
REQUIRED_LESSONS = (
    "config declaration order is not performance ranking",
    "a callable producer does not imply baseline consumption",
    "a candidate cannot be a delta from a nonexistent baseline rule",
    "soft confirmation cannot be inferred from a conceptual label",
    "candidate delta units must come from governed exposure semantics",
    "hard-veto completeness is an admission prerequisite, not a late replay detail",
)
REOPEN_EVIDENCE_TYPES = (
    "RECOVERY_PERMISSION_PIT_GOVERNED_PRODUCER",
    "BASELINE_RECOVERY_TRANSITION_CONSUMPTION_PATH",
    "RUNTIME_READY_AUTHORITATIVE_HARD_VETO_AGGREGATE",
    "RUNTIME_READY_GOVERNED_NATIVE_EXPOSURE_SCALAR",
    "INDEPENDENT_CALLABLE_NON_HARD_DEFENSIVE_REQUEST",
)

REQUIRED_CATALOG_REFERENCES = (
    "growth-tilt-candidate-family-close",
    "growth_tilt_candidate_family_closure.json",
    "growth_tilt_candidate_negative_result_ledger.json",
    "growth_tilt_candidate_family_closure.md",
)
REQUIRED_FLOW_REFERENCES = (
    "TRADING-2438N1",
    READY_STATUS,
    CLOSURE_STATUS,
    "pit_candidates_tested=0",
    "2 PASS/8 BLOCKED",
    "TRADING-2438N2",
)


def build_growth_tilt_candidate_family_closure(
    sources: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    as_of: str,
) -> dict[str, Any]:
    m1e = _mapping(sources.get("m1e"))
    adapters = _mapping(sources.get("adapters"))
    prerequisite = _mapping(m1e.get("prerequisite_matrix"))
    prerequisite_rows = [
        dict(item)
        for item in _sequence(prerequisite.get("rows"))
        if isinstance(item, Mapping)
    ]
    copied_prerequisite_matrix = {
        "schema_version": prerequisite.get("schema_version"),
        "replacement_candidate_id": prerequisite.get("replacement_candidate_id"),
        "rows": prerequisite_rows,
        "prerequisite_count": prerequisite.get("prerequisite_count"),
        "pass_count": prerequisite.get("pass_count"),
        "blocked_count": prerequisite.get("blocked_count"),
        "all_prerequisites_ready": prerequisite.get("all_prerequisites_ready"),
        "blocker_codes": list(_sequence(prerequisite.get("blocker_codes"))),
    }
    ledger = _negative_result_ledger(prerequisite)
    reopen_policy = _reopen_policy()
    strict_errors = _strict_validation_errors(
        sources,
        prerequisite_rows,
        copied_prerequisite_matrix,
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    status = BLOCKED_STATUS if strict_errors else READY_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438N1",
        "report_type": REPORT_TYPE,
        "status": status,
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "family_id": FAMILY_ID,
        "source_artifacts": [dict(item) for item in source_artifacts],
        "closure_status": CLOSURE_STATUS,
        "closure_reason_codes": list(CLOSURE_REASON_CODES),
        "candidate_dispositions": dict(EXPECTED_DISPOSITIONS),
        "replacement_a_prerequisite_matrix": copied_prerequisite_matrix,
        "prerequisite_pass_count": prerequisite.get("pass_count"),
        "prerequisite_blocked_count": prerequisite.get("blocked_count"),
        "baseline_adapter_ready_count": adapters.get("adapter_contract_ready_count"),
        "baseline_adapter_blocked_count": adapters.get(
            "adapter_contract_blocked_count"
        ),
        "negative_result_ledger": ledger,
        "reopen_policy": reopen_policy,
        "family_reopen_ready": False,
        "family_route_enabled": False,
        "closed_family_m2_route_disabled": True,
        "pit_candidates_tested": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": 0,
        "null_metrics_interpreted_as_fail": False,
        "runtime_metrics_materialized": False,
        "approved_candidate_count": 0,
        "m2_eligible_candidate_count": 0,
        "m2_eligible_candidate_ids": [],
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "data_quality_gate_executed": False,
        "data_quality_status": "NOT_APPLICABLE_GOVERNANCE_ARTIFACT_CLOSURE_ONLY",
        **_safety(),
    }


def _negative_result_ledger(
    prerequisite: Mapping[str, Any],
) -> dict[str, Any]:
    blocker_codes = [str(item) for item in _sequence(prerequisite.get("blocker_codes"))]
    records = [
        {
            "hypothesis_id": "recovery_reentry_speedup_guard",
            "intended_failure_target": "SLOW_GROWTH_RECOVERY_REENTRY",
            "original_candidate_role": "RECOVERY_REENTRY_TIMING_ACCELERATOR",
            "terminal_disposition": "REDEFINE",
            "non_executability_reason": "NO_GOVERNED_BASELINE_RECOVERY_CONSUMPTION_RULE",
            "missing_baseline_capabilities": [
                "RECOVERY_PRODUCER_PIT_LINEAGE",
                "BASELINE_RECOVERY_TRANSITION_CONSUMPTION",
                "BASELINE_RECOVERY_PERSISTENCE",
            ],
            "research_design_lesson": [
                REQUIRED_LESSONS[1],
                REQUIRED_LESSONS[2],
            ],
            "prohibited_future_reuse": [
                "DO_NOT_DESCRIBE_AS_BASELINE_SPEEDUP",
                "DO_NOT_CREATE_BASELINE_PERSISTENCE_TO_RESCUE_CANDIDATE",
            ],
            "reopen_condition": "INDEPENDENT_BASELINE_RECOVERY_CONSUMPTION_PROJECT",
        },
        {
            "hypothesis_id": "false_risk_off_confirmation_relaxation",
            "intended_failure_target": "FALSE_RISK_OFF_ENTRY",
            "original_candidate_role": "DEFENSIVE_ENTRY_SOFT_CONFIRMATION_GRACE",
            "terminal_disposition": "WITHDRAW",
            "non_executability_reason": (
                "NO_CALLABLE_SOFT_CONFIRMATION_OR_AGGREGATE_NON_HARD_REQUEST"
            ),
            "missing_baseline_capabilities": [
                "CALLABLE_PIT_SOFT_CONFIRMATION",
                "CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST",
            ],
            "research_design_lesson": [REQUIRED_LESSONS[3]],
            "prohibited_future_reuse": [
                "DO_NOT_INFER_SOFT_CONFIRMATION_FROM_CONCEPTUAL_LABEL",
                "DO_NOT_ADD_AGGREGATE_REQUEST_INSIDE_CANDIDATE_TASK",
            ],
            "reopen_condition": "INDEPENDENT_NON_HARD_DEFENSIVE_REQUEST_PROJECT",
        },
        {
            "hypothesis_id": "missed_upside_reentry_accelerator",
            "intended_failure_target": "MISSED_UPSIDE_AFTER_RECOVERY",
            "original_candidate_role": "RECOVERY_REENTRY_ACCELERATOR",
            "terminal_disposition": "REDEFINE",
            "non_executability_reason": (
                "NON_ORTHOGONAL_AND_NO_INDEPENDENT_EXECUTABLE_RAMP_CONTRACT"
            ),
            "missing_baseline_capabilities": [
                "READY_RAMP_RULE",
                "GOVERNED_NATIVE_EXPOSURE_SCALAR",
                "GOVERNED_REQUESTED_APPLIED_TRANSITION",
            ],
            "research_design_lesson": [
                REQUIRED_LESSONS[0],
                "structural orthogonality must precede candidate selection",
            ],
            "prohibited_future_reuse": [
                "DO_NOT_DUPLICATE_TRIGGER_TIMING_AXIS",
                "DO_NOT_CALL_DECLARATION_ORDER_A_PERFORMANCE_RANK",
            ],
            "reopen_condition": "INDEPENDENT_POST_CONFIRMATION_RAMP_CONTRACT",
        },
        {
            "hypothesis_id": "capped_recovery_permission_overlay",
            "intended_failure_target": "MISSED_UPSIDE_WITH_CAPPED_RECOVERY_PERMISSION",
            "original_candidate_role": "CAPPED_RECOVERY_PERMISSION_OVERLAY",
            "terminal_disposition": "KEEP_REDEFINED_BLOCKED",
            "non_executability_reason": "M1E_APPROVAL_PREREQUISITES_BLOCKED",
            "missing_baseline_capabilities": blocker_codes,
            "research_design_lesson": [
                REQUIRED_LESSONS[4],
                REQUIRED_LESSONS[5],
            ],
            "prohibited_future_reuse": [
                "DO_NOT_REUSE_QQQ_EQUIVALENT_AS_CANDIDATE_DELTA_UNIT",
                "DO_NOT_TREAT_PARTIAL_HARD_VETO_SET_AS_REPLAY_DETAIL",
                "DO_NOT_APPROVE_WITH_DEFAULT_OR_POST_REPLAY_THRESHOLD",
            ],
            "reopen_condition": "ALL_M1E_PREREQUISITES_PROVEN_BY_INDEPENDENT_BASELINE_WORK",
        },
    ]
    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "record_count": len(records),
        "records": records,
        "required_research_design_lessons": list(REQUIRED_LESSONS),
        "configuration_order_is_performance_rank": False,
        "new_baseline_behavior_allowed": False,
    }


def _reopen_policy() -> dict[str, Any]:
    return {
        "schema_version": REOPEN_SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "status": "CLOSED_PENDING_INDEPENDENT_BASELINE_EVIDENCE",
        "allowed_new_evidence_types": list(REOPEN_EVIDENCE_TYPES),
        "minimum_independent_evidence_type_count": 1,
        "new_evidence_refs": [],
        "baseline_change_task_ids": [],
        "baseline_change_motivation": None,
        "candidate_independent_change": True,
        "candidate_work_may_motivate_baseline_change": False,
        "owner_reapproval_required": True,
        "screening_policy_refreeze_required": True,
        "reopen_ready": False,
    }


def _strict_validation_errors(
    sources: Mapping[str, Any],
    prerequisite_rows: Sequence[Mapping[str, Any]],
    copied_prerequisite_matrix: Mapping[str, Any],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> list[str]:
    errors: list[str] = []
    m1e = _mapping(sources.get("m1e"))
    adapters = _mapping(sources.get("adapters"))
    owner = _mapping(sources.get("owner_resolution"))
    candidate_set = _mapping(sources.get("candidate_set"))
    expected = (
        (m1e.get("schema_version"), EXPECTED_M1E_SCHEMA, "m1e_schema_mismatch"),
        (m1e.get("status"), EXPECTED_M1E_STATUS, "m1e_status_mismatch"),
        (
            adapters.get("schema_version"),
            EXPECTED_ADAPTER_SCHEMA,
            "adapter_schema_mismatch",
        ),
        (
            adapters.get("status"),
            EXPECTED_ADAPTER_STATUS,
            "adapter_status_mismatch",
        ),
        (
            owner.get("schema_version"),
            EXPECTED_OWNER_SCHEMA,
            "owner_resolution_schema_mismatch",
        ),
        (
            candidate_set.get("schema_version"),
            EXPECTED_CANDIDATE_SET_SCHEMA,
            "candidate_set_schema_mismatch",
        ),
    )
    for actual, wanted, code in expected:
        if actual != wanted:
            errors.append(code)
    if m1e.get("disposition") != "KEEP_REDEFINED_BLOCKED":
        errors.append("m1e_disposition_mismatch")
    if m1e.get("approved_candidate_count") != 0 or m1e.get(
        "m2_eligible_candidate_count"
    ) != 0:
        errors.append("m1e_zero_candidate_invariant_failed")
    if adapters.get("adapter_contract_ready_count") != 0 or adapters.get(
        "adapter_contract_blocked_count"
    ) != 4:
        errors.append("adapter_zero_ready_four_blocked_invariant_failed")
    ids = [str(item.get("prerequisite_id")) for item in prerequisite_rows]
    if tuple(ids) != EXPECTED_PREREQUISITE_IDS:
        errors.append("m1e_prerequisite_identity_or_order_mismatch")
    prerequisite = _mapping(m1e.get("prerequisite_matrix"))
    if prerequisite.get("pass_count") != 2 or prerequisite.get("blocked_count") != 8:
        errors.append("m1e_prerequisite_count_mismatch")
    if tuple(_sequence(prerequisite.get("blocker_codes"))) != EXPECTED_BLOCKER_CODES:
        errors.append("m1e_blocker_identity_or_order_mismatch")
    if copied_prerequisite_matrix != prerequisite:
        errors.append("m1e_prerequisite_copy_not_exact")
    report_ids = {
        str(item.get("report_id"))
        for item in _sequence(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    if REPORT_TYPE not in report_ids:
        errors.append("report_registry_alignment_failed")
    if not all(item in artifact_catalog_text for item in REQUIRED_CATALOG_REFERENCES):
        errors.append("artifact_catalog_alignment_failed")
    if not all(item in system_flow_text for item in REQUIRED_FLOW_REFERENCES):
        errors.append("system_flow_alignment_failed")
    if not all(
        item in requirement_text
        for item in (
            "TRADING-2438N1",
            CLOSURE_STATUS,
            "candidate-independent baseline project",
            "zero placeholder candidates",
        )
    ):
        errors.append("requirement_alignment_failed")
    return sorted(set(errors))


def _safety() -> dict[str, Any]:
    return {
        "closure_only": True,
        "runtime_code_invoked": False,
        "replay_run": False,
        "runtime_metrics_generated": False,
        "baseline_behavior_changed": False,
        "candidate_behavior_implemented": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "production_allowed": False,
        "production_enabled": False,
        "portfolio_weight_mutated": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
