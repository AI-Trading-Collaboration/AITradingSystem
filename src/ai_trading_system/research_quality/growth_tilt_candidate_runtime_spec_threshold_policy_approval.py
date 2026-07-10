from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from ai_trading_system.research_quality import (
    growth_tilt_owner_mapping_inventory as mapping_inventory,
)
from ai_trading_system.research_quality import (
    growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution as resolution,
)

REQUIRED_METRIC_IDS = resolution.REQUIRED_METRIC_IDS
SUPPORTED_OPERATORS = resolution.SUPPORTED_OPERATORS
EXPECTED_CANDIDATE_IDS = (
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator",
)
EXPECTED_M2_OPERATION_TYPES = {
    "recovery_reentry_speedup_guard": "EARLY_REENTRY_PROVISIONAL_EXPOSURE",
    "false_risk_off_confirmation_relaxation": "DEFENSIVE_SOFT_CONFIRMATION_GRACE",
}
EXPECTED_METRIC_CONTRACT_ID = "growth_tilt_candidate_replay_metric_contract_v1"
EXPECTED_THRESHOLD_POLICY_ID = "growth_tilt_candidate_pit_screening_policy_v1"

SCHEMA_VERSION = "growth_tilt_candidate_runtime_spec_threshold_policy_approval.v2"
CANDIDATE_MATRIX_SCHEMA_VERSION = "growth_tilt_candidate_runtime_spec_review_matrix.v2"
METRIC_MATRIX_SCHEMA_VERSION = "growth_tilt_metric_contract_review_matrix.v2"
THRESHOLD_MATRIX_SCHEMA_VERSION = "growth_tilt_threshold_policy_review_matrix.v2"
OWNER_CHECKLIST_SCHEMA_VERSION = "growth_tilt_runtime_spec_owner_action_checklist.v2"
OWNER_VALIDATION_SCHEMA_VERSION = "growth_tilt_owner_review_validation.v1"
APPROVED_SPECS_SCHEMA_VERSION = "growth_tilt_approved_candidate_runtime_specs.v1"
NO_EFFECT_SCHEMA_VERSION = "growth_tilt_runtime_spec_threshold_approval_no_effect.v2"

REPORT_TYPE = "growth_tilt_candidate_runtime_spec_threshold_policy_approval"
READY_STATUS = "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVED"
BLOCKED_STATUS = "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_BLOCKED"
# Kept as import-compatible historical constants. Mixed decisions no longer make the
# whole candidate set take either status.
REDEFINE_STATUS = (
    "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_"
    "CANDIDATE_REDEFINITION_REQUIRED"
)
WITHDRAW_STATUS = (
    "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_"
    "WITHDRAWAL_RESELECTION_REQUIRED"
)

EXPECTED_SOURCE_STATUS = (
    "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED"
)
EXPECTED_SOURCE_ROUTE = (
    "TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval"
)
EXPECTED_SOURCE_SCHEMA = "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.v1"
EXPECTED_OWNER_REVIEW_SCHEMA = (
    "growth_tilt_candidate_runtime_spec_threshold_policy_review.v2"
)
EXPECTED_METRIC_CONTRACT_SCHEMA = "growth_tilt_candidate_replay_metric_contract.v1"
EXPECTED_THRESHOLD_POLICY_SCHEMA = "growth_tilt_candidate_pit_screening_policy.v1"

NEXT_ROUTE_READY = "TRADING-2438M2A_GROWTH_TILT_TYPED_OVERLAY_EXECUTOR"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2438M1B_GROWTH_TILT_SHARED_METRIC_AND_SCREENING_POLICY_APPROVAL"
)
NEXT_ROUTE_DECISION = "TRADING-2438M1A_GROWTH_TILT_OWNER_DECISION_FINALIZATION"
NEXT_ROUTE_REDEFINE = "TRADING-2438M1A_GROWTH_TILT_CANDIDATE_C_SECOND_OWNER_REVIEW"
NEXT_ROUTE_WITHDRAW = NEXT_ROUTE_DECISION
NEXT_ROUTE_NO_APPROVED = "TRADING-2438N_GROWTH_TILT_NO_APPROVED_CANDIDATE_DISPOSITION"

ALLOWED_DECISIONS = {"PENDING", "APPROVE", "REDEFINE", "WITHDRAW"}
REQUIRED_DECISION_FIELDS = (
    "decision_owner",
    "decision_version",
    "decision_timestamp",
    "decision_rationale",
    "review_condition",
    "expiry_condition",
    "next_route",
)
REQUIRED_RUNTIME_SPEC_FIELDS = (
    "candidate_role",
    "baseline_config_ref",
    "operation_type",
    "parameters",
    "hard_veto_ids",
    "applicable_regime_ids",
    "expiry_conditions",
    "rollback_conditions",
)
REQUIRED_EXECUTOR_MAPPING_FIELDS = (
    "executor_family",
    "operation_type",
    "planned_entrypoint",
    "input_contract_version",
    "output_contract_version",
)
REQUIRED_REDEFINITION_FIELDS = (
    "old_candidate_id",
    "proposed_candidate_id",
    "overlap_with",
    "old_semantics_rejected_reason",
    "new_candidate_role",
    "changes_trigger_timing",
    "changes_ramp_speed",
    "rationale",
    "second_owner_approval_required",
)

DECISION_PENDING = "OWNER_CANDIDATE_DECISION_PENDING"
DECISION_INVALID = "OWNER_CANDIDATE_DECISION_INVALID"
REVIEW_METADATA_INCOMPLETE = "OWNER_CANDIDATE_REVIEW_METADATA_INCOMPLETE"
RUNTIME_SPEC_INCOMPLETE = "OWNER_RUNTIME_SPEC_INCOMPLETE"
EXECUTOR_MAPPING_INCOMPLETE = "OWNER_EXECUTOR_MAPPING_INCOMPLETE"
METRIC_CONTRACT_INCOMPLETE = "OWNER_SHARED_METRIC_CONTRACT_INCOMPLETE"
THRESHOLD_POLICY_INCOMPLETE = "OWNER_SCREENING_THRESHOLD_POLICY_INCOMPLETE"
CANDIDATE_REDEFINITION_INCOMPLETE = "OWNER_CANDIDATE_REDEFINITION_INCOMPLETE"
THRESHOLD_VALUE_INVALID = "OWNER_THRESHOLD_VALUE_INVALID"
THRESHOLD_OPERATOR_UNSUPPORTED = "OWNER_THRESHOLD_OPERATOR_UNSUPPORTED"
THRESHOLD_METRIC_BINDING_INVALID = "OWNER_THRESHOLD_METRIC_BINDING_INVALID"
CANDIDATE_REDEFINITION_REQUIRED = "OWNER_CANDIDATE_REDEFINITION_SECOND_REVIEW_REQUIRED"
CANDIDATE_WITHDRAWAL_REQUIRED = "OWNER_CANDIDATE_WITHDRAWAL_RECORDED"
SAFETY_BOUNDARY_INVALID = "OWNER_REVIEW_SAFETY_BOUNDARY_INVALID"

REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "growth-tilt-candidate-runtime-spec-threshold-policy-approval",
    "growth_tilt_candidate_replay_metric_contract.yaml",
    "growth_tilt_candidate_pit_screening_policy.yaml",
    "owner_review_validation.json",
    "approved_candidate_runtime_specs.json",
)
REQUIRED_FLOW_REFERENCES: tuple[str, ...] = (
    "TRADING-2438M1A",
    "TRADING-2438M1B",
    "CONFIG_DECLARATION_ORDER",
    "performance_ranked=false",
    READY_STATUS,
    BLOCKED_STATUS,
    NEXT_ROUTE_READY,
)


def build_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
    source_2438m: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    *,
    metric_contract: Mapping[str, Any] | None = None,
    threshold_policy: Mapping[str, Any] | None = None,
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    as_of: str,
) -> dict[str, Any]:
    metric_contract = metric_contract or {}
    threshold_policy = threshold_policy or {}
    source_candidate_ids = _candidate_ids(source_2438m)
    review_candidate_ids = [
        str(item) for item in _sequence(owner_review.get("candidate_ids")) if item
    ]
    review_rows = _sequence(owner_review.get("candidate_reviews"))
    reviews_by_id = _records_by_id(review_rows)
    documentation_alignment = _documentation_alignment(
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    metric_rows, shared_metric_ready, metric_blockers = _metric_contract_status(
        metric_contract
    )
    threshold_rows, threshold_ready_by_candidate, threshold_blockers = (
        _threshold_policy_status(threshold_policy, metric_contract)
    )
    strict_errors = _strict_validation_errors(
        source_2438m,
        owner_review,
        metric_contract,
        threshold_policy,
        source_candidate_ids,
        review_candidate_ids,
        review_rows,
        documentation_alignment,
        as_of,
    )

    candidate_results: list[dict[str, Any]] = []
    owner_actions: list[dict[str, Any]] = []
    approved_specs: list[dict[str, Any]] = []
    for selection_order, candidate_id in enumerate(source_candidate_ids, start=1):
        result, actions, approved_spec = _review_candidate(
            candidate_id,
            selection_order,
            reviews_by_id.get(candidate_id, {}),
            shared_metric_ready=shared_metric_ready,
            metric_blockers=metric_blockers,
            threshold_policy_ready=threshold_ready_by_candidate.get(candidate_id, False),
            threshold_blockers=threshold_blockers.get(candidate_id, []),
        )
        candidate_results.append(result)
        owner_actions.extend(actions)
        if approved_spec:
            approved_specs.append(approved_spec)

    status = _overall_status(candidate_results, strict_errors)
    next_route = _next_route(status, candidate_results)
    decision_counts = Counter(str(item.get("decision")) for item in candidate_results)
    gap_counts = Counter(str(item.get("gap_code")) for item in owner_actions)
    source_ready = _source_ready(source_2438m, source_candidate_ids)
    candidate_selection = _mapping(owner_review.get("candidate_selection"))
    requirements = _requirements(
        source_ready=source_ready,
        candidate_identity_ready=(
            source_candidate_ids == review_candidate_ids
            and tuple(source_candidate_ids) == EXPECTED_CANDIDATE_IDS
            and len(source_candidate_ids) == len(reviews_by_id) == 3
        ),
        selection_semantics_ready=_selection_semantics_ready(candidate_selection),
        safety_ready=_safety_boundary_ready(owner_review),
        documentation_ready=all(documentation_alignment.values()),
    )
    gaps = [item for item in requirements if item["status"] != "PASS"]
    safety = _safety()
    m2_ids = [
        str(item["candidate_id"])
        for item in candidate_results
        if item.get("m2_eligible") is True
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M1",
        "report_type": REPORT_TYPE,
        "status": status,
        "readiness_status": status,
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": "2022-12-01",
        "source_status": source_2438m.get("status"),
        "source_run_id": source_2438m.get("run_id"),
        "source_artifacts": [dict(item) for item in source_artifacts],
        "source_2438m_ready_for_owner_review": source_ready,
        "candidate_selection": dict(candidate_selection),
        "candidate_count": len(candidate_results),
        "candidate_ids": source_candidate_ids,
        "selection_basis": candidate_selection.get("selection_basis"),
        "performance_ranked": candidate_selection.get("performance_ranked"),
        "owner_review_status": owner_review.get("owner_review_status"),
        "owner_decision_counts": dict(sorted(decision_counts.items())),
        "owner_decision_complete_count": sum(
            item.get("decision_complete") is True for item in candidate_results
        ),
        "approved_candidate_count": _decision_count(candidate_results, "APPROVE"),
        "pending_candidate_count": _decision_count(candidate_results, "PENDING"),
        "redefine_candidate_count": _decision_count(candidate_results, "REDEFINE"),
        "withdraw_candidate_count": _decision_count(candidate_results, "WITHDRAW"),
        "runtime_spec_ready_count": sum(
            item.get("runtime_spec_ready") is True for item in candidate_results
        ),
        "approved_metric_contract_count": (
            len(REQUIRED_METRIC_IDS) if shared_metric_ready else 0
        ),
        "metric_contract_ready_count": sum(
            item.get("metric_contract_ready") is True for item in candidate_results
        ),
        "approved_threshold_policy_count": sum(
            item.get("threshold_policy_ready") is True for item in candidate_results
        ),
        "threshold_policy_ready_count": sum(
            item.get("threshold_policy_ready") is True for item in candidate_results
        ),
        "m2_eligible_candidate_count": len(m2_ids),
        "m2_eligible_candidate_ids": m2_ids,
        "candidate_reviews": candidate_results,
        "owner_input_gap_count": len(owner_actions),
        "owner_input_gaps_by_code": dict(sorted(gap_counts.items())),
        "recommended_next_research_task": next_route,
        "next_route": next_route,
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "documentation_alignment": documentation_alignment,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "candidate_runtime_spec_review_matrix": {
            "schema_version": CANDIDATE_MATRIX_SCHEMA_VERSION,
            "status": status,
            "candidate_reviews": candidate_results,
        },
        "metric_contract_review_matrix": {
            "schema_version": METRIC_MATRIX_SCHEMA_VERSION,
            "status": status,
            "contract_id": metric_contract.get("contract_id"),
            "contract_status": metric_contract.get("status"),
            "contract_ready": shared_metric_ready,
            "required_metric_ids": list(REQUIRED_METRIC_IDS),
            "rows": metric_rows,
            "blocker_codes": metric_blockers,
        },
        "threshold_policy_review_matrix": {
            "schema_version": THRESHOLD_MATRIX_SCHEMA_VERSION,
            "status": status,
            "policy_id": threshold_policy.get("policy_id"),
            "policy_status": threshold_policy.get("policy_status"),
            "supported_operators": sorted(SUPPORTED_OPERATORS),
            "rows": threshold_rows,
            "ready_by_candidate": threshold_ready_by_candidate,
            "blockers_by_candidate": threshold_blockers,
        },
        "owner_review_validation": {
            "schema_version": OWNER_VALIDATION_SCHEMA_VERSION,
            "status": status,
            "strict_validation_errors": strict_errors,
            "owner_input_gap_count": len(owner_actions),
            "actions": owner_actions,
        },
        "approved_candidate_runtime_specs": {
            "schema_version": APPROVED_SPECS_SCHEMA_VERSION,
            "status": status,
            "candidate_count": len(approved_specs),
            "candidate_ids": m2_ids,
            "candidate_specs": approved_specs,
        },
        "owner_action_checklist": {
            "schema_version": OWNER_CHECKLIST_SCHEMA_VERSION,
            "status": status,
            "open_action_count": len(owner_actions),
            "actions": owner_actions,
        },
        "no_effect_boundary": {
            "schema_version": NO_EFFECT_SCHEMA_VERSION,
            "status": status,
            **safety,
        },
        "data_quality_gate_executed": False,
        "data_quality_gate_passed": None,
        "data_quality_status": (
            "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACTS_CONFIG_OWNER_REVIEW_ONLY"
        ),
        "data_quality_gate_reason": (
            "M1 validates prior evidence, governed config, and owner-review input only; "
            "it does not read cached market or outcome data."
        ),
        **safety,
    }


def _review_candidate(
    candidate_id: str,
    selection_order: int,
    review: Mapping[str, Any],
    *,
    shared_metric_ready: bool,
    metric_blockers: Sequence[str],
    threshold_policy_ready: bool,
    threshold_blockers: Sequence[str],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
    decision = str(review.get("decision") or "PENDING").upper()
    actions: list[dict[str, Any]] = []
    metadata_missing = [field for field in REQUIRED_DECISION_FIELDS if not review.get(field)]
    selection_ready = bool(
        review.get("selection_order") == selection_order
        and review.get("selection_basis") == "CONFIG_DECLARATION_ORDER"
    )
    if not selection_ready:
        metadata_missing.extend(["selection_order", "selection_basis"])
    metadata_missing = sorted(set(metadata_missing))

    if decision not in ALLOWED_DECISIONS:
        actions.append(
            _action(candidate_id, DECISION_INVALID, "decision", "Record a supported decision.")
        )
    elif decision == "PENDING":
        actions.append(
            _action(
                candidate_id,
                DECISION_PENDING,
                "decision",
                "Record APPROVE, REDEFINE, or WITHDRAW before M2 eligibility evaluation.",
            )
        )
    if decision != "PENDING" and metadata_missing:
        actions.append(
            _action(
                candidate_id,
                REVIEW_METADATA_INCOMPLETE,
                "candidate_review",
                f"Complete decision metadata: {', '.join(metadata_missing)}.",
            )
        )

    runtime_spec_ready = False
    executor_mapping_ready = False
    redefinition_ready = False
    metric_ready = False
    threshold_ready = False
    runtime_missing: list[str] = []
    executor_missing: list[str] = []

    if decision == "APPROVE":
        runtime_spec = _mapping(review.get("runtime_spec"))
        runtime_spec_ready, runtime_missing = _runtime_spec_status(
            candidate_id, runtime_spec
        )
        executor_mapping = _mapping(review.get("executor_mapping"))
        executor_mapping_ready, executor_missing = _executor_mapping_status(
            candidate_id, executor_mapping
        )
        metric_ready = bool(
            review.get("metric_contract_ref") == EXPECTED_METRIC_CONTRACT_ID
            and shared_metric_ready
        )
        threshold_ready = bool(
            review.get("threshold_policy_ref") == EXPECTED_THRESHOLD_POLICY_ID
            and threshold_policy_ready
        )
        if not runtime_spec_ready:
            actions.append(
                _action(
                    candidate_id,
                    RUNTIME_SPEC_INCOMPLETE,
                    "runtime_spec",
                    f"Resolve runtime fields/placeholders: {', '.join(runtime_missing)}.",
                )
            )
        if not executor_mapping_ready:
            actions.append(
                _action(
                    candidate_id,
                    EXECUTOR_MAPPING_INCOMPLETE,
                    "executor_mapping",
                    f"Resolve executor fields: {', '.join(executor_missing)}.",
                )
            )
        if not metric_ready:
            action_blockers = list(metric_blockers)
            if review.get("metric_contract_ref") != EXPECTED_METRIC_CONTRACT_ID:
                action_blockers.append("metric_contract_ref_mismatch")
            actions.append(
                _action(
                    candidate_id,
                    METRIC_CONTRACT_INCOMPLETE,
                    "metric_contract_ref",
                    "Approve the shared metric contract: "
                    f"{', '.join(sorted(set(action_blockers)))}.",
                )
            )
        if not threshold_ready:
            action_blockers = list(threshold_blockers)
            if review.get("threshold_policy_ref") != EXPECTED_THRESHOLD_POLICY_ID:
                action_blockers.append("threshold_policy_ref_mismatch")
            actions.append(
                _action(
                    candidate_id,
                    THRESHOLD_POLICY_INCOMPLETE,
                    "threshold_policy_ref",
                    "Preregister the candidate screening policy: "
                    f"{', '.join(sorted(set(action_blockers)))}.",
                )
            )
    elif decision == "REDEFINE":
        redefinition_ready, redefinition_missing = _redefinition_status(
            candidate_id, _mapping(review.get("redefinition"))
        )
        if not redefinition_ready:
            actions.append(
                _action(
                    candidate_id,
                    CANDIDATE_REDEFINITION_INCOMPLETE,
                    "redefinition",
                    f"Complete redefinition fields: {', '.join(redefinition_missing)}.",
                )
            )
    elif decision == "WITHDRAW":
        redefinition_ready = True

    decision_complete = bool(
        decision in {"APPROVE", "REDEFINE", "WITHDRAW"} and not metadata_missing
        and (decision != "REDEFINE" or redefinition_ready)
    )
    m2_eligible = bool(
        decision == "APPROVE"
        and decision_complete
        and runtime_spec_ready
        and executor_mapping_ready
        and metric_ready
        and threshold_ready
    )
    if m2_eligible:
        review_status = "APPROVED_FOR_PIT_REPLAY"
    elif decision == "APPROVE":
        review_status = "APPROVAL_CONTRACT_BLOCKED"
    elif decision == "REDEFINE" and redefinition_ready:
        review_status = "REDEFINED_SECOND_OWNER_APPROVAL_REQUIRED"
    elif decision == "REDEFINE":
        review_status = "REDEFINITION_INCOMPLETE"
    elif decision == "WITHDRAW" and decision_complete:
        review_status = "WITHDRAWN"
    elif decision == "PENDING":
        review_status = "PENDING"
    else:
        review_status = "INVALID_OR_INCOMPLETE"

    result = {
        "candidate_id": candidate_id,
        "selection_order": selection_order,
        "selection_basis": "CONFIG_DECLARATION_ORDER",
        "performance_ranked": False,
        "decision": decision,
        "review_status": review_status,
        "decision_owner": review.get("decision_owner"),
        "decision_version": review.get("decision_version"),
        "decision_timestamp": review.get("decision_timestamp"),
        "decision_rationale": review.get("decision_rationale"),
        "decision_complete": decision_complete,
        "runtime_spec_ready": runtime_spec_ready,
        "executor_mapping_ready": executor_mapping_ready,
        "metric_contract_ready": metric_ready,
        "threshold_policy_ready": threshold_ready,
        "redefinition_ready": redefinition_ready,
        "m2_eligible": m2_eligible,
        "gap_codes": sorted(
            {
                str(item.get("gap_code"))
                for item in actions
                if item.get("candidate_id") == candidate_id
            }
        ),
    }
    approved_spec = None
    if m2_eligible:
        approved_spec = {
            "candidate_id": candidate_id,
            "selection_order": selection_order,
            "selection_basis": "CONFIG_DECLARATION_ORDER",
            "performance_ranked": False,
            "runtime_spec": dict(_mapping(review.get("runtime_spec"))),
            "executor_mapping": dict(_mapping(review.get("executor_mapping"))),
            "metric_contract_ref": review.get("metric_contract_ref"),
            "threshold_policy_ref": review.get("threshold_policy_ref"),
            "decision_version": review.get("decision_version"),
        }
    return result, actions, approved_spec


def _runtime_spec_status(
    candidate_id: str, spec: Mapping[str, Any]
) -> tuple[bool, list[str]]:
    missing = _missing_required_fields(spec, REQUIRED_RUNTIME_SPEC_FIELDS)
    missing.extend(_owner_placeholder_paths(spec))
    expected_operation = EXPECTED_M2_OPERATION_TYPES.get(candidate_id)
    if expected_operation and spec.get("operation_type") != expected_operation:
        missing.append("operation_type_semantic_mismatch")
    parameters = _mapping(spec.get("parameters"))
    if candidate_id == "recovery_reentry_speedup_guard":
        invariants = {
            "recovery_signal_inventory_status": "PIT_APPROVED_CALLABLE",
            "recovery_persistence_inventory_status": "GOVERNED_BASELINE_RULE_RESOLVED",
            "lead_steps": 1,
            "baseline_transition_state": "defensive",
            "provisional_target_state": "neutral",
            "excluded_source_state": "risk_off",
            "provisional_exposure_fraction_of_remaining_gap": 0.25,
            "provisional_exposure_absolute_cap": 0.05,
            "provisional_exposure_unit": "QQQ_EQUIVALENT_WEIGHT",
            "max_active_steps": 2,
            "confirmed_state_ramp_multiplier": 1.0,
            "target_exposure_override_allowed": False,
            "hard_veto_bypass_allowed": False,
        }
    else:
        invariants = {
            "soft_confirmation_inventory_status": "EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION",
            "relaxation_mode": "ONE_STEP_GRACE",
            "grace_steps": 1,
            "remove_confirmation_entirely": False,
            "defensive_exposure_override_allowed": False,
            "hard_veto_bypass_allowed": False,
            "max_active_steps": 1,
        }
    for field, expected in invariants.items():
        if parameters.get(field) != expected:
            missing.append(f"parameters.{field}_invariant")
    if spec.get("baseline_mapping_status") != mapping_inventory.M2_READY_STATUS:
        missing.append("baseline_mapping_status")
    if tuple(spec.get("hard_veto_ids") or ()) != mapping_inventory.EXPECTED_VETO_IDS:
        missing.append("hard_veto_ids_complete_callable_pit_set")
    if spec.get("hard_veto_set_inventory_status") != "COMPLETE_CALLABLE_PIT_VALID_SET":
        missing.append("hard_veto_set_inventory_status")
    if spec.get("governed_transition_scope_inventory_status") != "RESOLVED":
        missing.append("governed_transition_scope_inventory_status")
    if spec.get("qqq_equivalent_binding_inventory_status") != "RESOLVED":
        missing.append("qqq_equivalent_binding_inventory_status")
    return not missing, sorted(set(missing))


def _executor_mapping_status(
    candidate_id: str, mapping: Mapping[str, Any]
) -> tuple[bool, list[str]]:
    missing = _missing_required_fields(mapping, REQUIRED_EXECUTOR_MAPPING_FIELDS)
    missing.extend(_owner_placeholder_paths(mapping))
    if mapping.get("executor_family") != "GrowthTiltCandidateOverlayExecutor":
        missing.append("executor_family")
    if mapping.get("operation_type") != EXPECTED_M2_OPERATION_TYPES.get(candidate_id):
        missing.append("operation_type_semantic_mismatch")
    return not missing, sorted(set(missing))


def _redefinition_status(
    candidate_id: str, redefinition: Mapping[str, Any]
) -> tuple[bool, list[str]]:
    missing = _missing_required_fields(redefinition, REQUIRED_REDEFINITION_FIELDS)
    missing.extend(_owner_placeholder_paths(redefinition))
    if candidate_id == "false_risk_off_confirmation_relaxation":
        expected = {
            "old_candidate_id": candidate_id,
            "proposed_candidate_id": "non_hard_defensive_entry_persistence_guard",
            "overlap_with": "BASELINE_NON_HARD_DEFENSIVE_ENTRY_REQUEST",
            "new_candidate_role": "NON_HARD_DEFENSIVE_ENTRY_PERSISTENCE_GUARD",
            "changes_trigger_timing": True,
            "changes_ramp_speed": False,
            "second_owner_approval_required": True,
        }
        parameter_invariants = {
            "trigger_source": "EXACT_BASELINE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST",
            "candidate_required_steps_expression": "baseline_required_steps_plus_one",
            "maximum_added_steps": 1,
            "changes_soft_component": False,
            "changes_aggregate_non_hard_request_persistence": True,
            "hard_veto_bypass_allowed": False,
            "auto_extension_allowed": False,
            "exposure_above_pre_request_baseline_allowed": False,
            "aggregate_request_contract_status": (
                "BLOCKED_NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST"
            ),
        }
    else:
        expected = {
            "old_candidate_id": candidate_id,
            "proposed_candidate_id": "post_confirmation_reentry_ramp_accelerator",
            "overlap_with": "recovery_reentry_speedup_guard",
            "new_candidate_role": "POST_CONFIRMATION_EXPOSURE_RAMP_ACCELERATOR",
            "changes_trigger_timing": False,
            "changes_ramp_speed": True,
            "second_owner_approval_required": True,
        }
        parameter_invariants = {
            "trigger_source": "EXACT_BASELINE_RECOVERY_CONFIRMATION",
            "trigger_lead_steps": 0,
            "target_exposure_override_allowed": False,
            "hard_veto_bypass_allowed": False,
            "reset_to_baseline_ramp_on_veto": True,
        }
    for field, value in expected.items():
        if redefinition.get(field) != value:
            missing.append(f"{field}_orthogonality_invariant")
    parameters = _mapping(redefinition.get("proposed_parameters"))
    for field, value in parameter_invariants.items():
        if parameters.get(field) != value:
            missing.append(f"proposed_parameters.{field}_orthogonality_invariant")
    return not missing, sorted(set(missing))


def _metric_contract_status(
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], bool, list[str]]:
    blockers: list[str] = []
    if contract.get("contract_id") != EXPECTED_METRIC_CONTRACT_ID:
        blockers.append("metric_contract_id_mismatch")
    if contract.get("status") != "APPROVED":
        blockers.append("metric_contract_not_owner_approved")
    if not _governed_text(contract.get("owner")):
        blockers.append("metric_contract_owner_unresolved")
    epsilon = _mapping(contract.get("relative_delta_epsilon_policy"))
    expected_relative_delta_policy = {
        "numerical_epsilon": 1.0e-12,
        "minimum_semantic_denominator": 1.0e-8,
        "denominator_below_minimum": "BLOCKED_BASELINE_MEASURE_TOO_SMALL",
        "use_epsilon_as_substitute_value": False,
    }
    if epsilon.get("status") != "APPROVED" or any(
        epsilon.get(field) != expected
        for field, expected in expected_relative_delta_policy.items()
    ):
        blockers.append("relative_delta_epsilon_policy_unresolved")
    if not _governed_text(epsilon.get("owner")):
        blockers.append("relative_delta_epsilon_owner_unresolved")
    empty_policy = _mapping(contract.get("empty_event_policy"))
    expected_empty_event_policy = {
        "status": "APPROVED",
        "minimum_primary_event_count": 5,
        "no_eligible_events": "BLOCKED_NO_ELIGIBLE_EVENTS",
        "eligible_but_fewer_than_minimum": "BLOCKED_INSUFFICIENT_PRIMARY_EVENTS",
        "both_candidate_and_baseline_zero_events": "BLOCKED_NO_ELIGIBLE_EVENTS",
        "empty_events_equal_zero_improvement": False,
    }
    if any(
        empty_policy.get(field) != expected
        for field, expected in expected_empty_event_policy.items()
    ) or not _governed_text(empty_policy.get("owner")):
        blockers.append("empty_event_policy_unresolved")

    metrics = _sequence(contract.get("metrics"))
    by_id = _records_by_key(metrics, "metric_id")
    metric_ids = [
        str(item.get("metric_id"))
        for item in metrics
        if isinstance(item, Mapping) and item.get("metric_id")
    ]
    if tuple(metric_ids) != tuple(REQUIRED_METRIC_IDS):
        blockers.append("metric_identity_or_order_mismatch")
    rows: list[dict[str, Any]] = []
    for metric_id in REQUIRED_METRIC_IDS:
        metric = _mapping(by_id.get(metric_id))
        missing = _missing_required_fields(
            metric,
            (
                "definition",
                "unit",
                "direction",
                "calculator_id",
                "calculator_version",
                "required_inputs",
                "missing_policy",
            ),
        )
        missing.extend(_owner_placeholder_paths(_mapping(metric.get("owner_fields"))))
        for path, value in _leaf_items(_mapping(metric.get("owner_fields"))):
            if value is None:
                missing.append(f"owner_fields.{path}")
        ready = not missing
        if not ready:
            blockers.append(f"metric_definition_incomplete:{metric_id}")
        rows.append(
            {
                "metric_id": metric_id,
                "definition": metric.get("definition"),
                "unit": metric.get("unit"),
                "direction": metric.get("direction"),
                "calculator_id": metric.get("calculator_id"),
                "calculator_version": metric.get("calculator_version"),
                "ready": ready,
                "missing_fields": sorted(set(missing)),
            }
        )
    provenance = set(
        str(item)
        for item in _sequence(contract.get("common_runtime_provenance_fields"))
    )
    required_provenance = {
        "metric_id",
        "value",
        "unit",
        "finite",
        "numerator",
        "denominator",
        "baseline_value",
        "candidate_value",
        "sample_count",
        "event_count",
        "window_start",
        "window_end",
        "as_of",
        "source_artifact_refs",
        "contract_version",
        "calculator_version",
        "status",
        "blocker_codes",
    }
    if not required_provenance.issubset(provenance):
        blockers.append("metric_runtime_provenance_incomplete")
    blockers = sorted(set(blockers))
    return rows, bool(rows) and not blockers, blockers


def _threshold_policy_status(
    policy: Mapping[str, Any], metric_contract: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, bool], dict[str, list[str]]]:
    global_blockers: list[str] = []
    if policy.get("policy_id") != EXPECTED_THRESHOLD_POLICY_ID:
        global_blockers.append("threshold_policy_id_mismatch")
    if policy.get("policy_status") != "APPROVED":
        global_blockers.append("threshold_policy_not_owner_approved")
    if not _governed_text(policy.get("owner")):
        global_blockers.append("threshold_policy_owner_unresolved")
    if not _sequence(policy.get("validation_evidence")):
        global_blockers.append("threshold_policy_validation_evidence_missing")
    if not _governed_text(policy.get("approved_at")):
        global_blockers.append("threshold_policy_approved_at_missing")
    if not _commit_identifier(policy.get("approved_commit")):
        global_blockers.append("threshold_policy_approved_commit_missing")
    if not _sha256_identifier(policy.get("source_hash")):
        global_blockers.append("threshold_policy_source_hash_missing")
    if policy.get("result_visibility_at_approval") != "NONE":
        global_blockers.append("threshold_policy_result_visibility_not_none")
    for field in ("rationale", "review_condition", "expiry_condition"):
        if not policy.get(field):
            global_blockers.append(f"threshold_policy_{field}_missing")
    readiness = _mapping(policy.get("readiness_gate"))
    expected_readiness = {
        "required_metric_count": 6,
        "computed_metric_count": 6,
        "finite_metric_count": 6,
        "threshold_evaluation_count": 6,
        "pit_valid": True,
        "baseline_and_candidate_comparable": True,
        "minimum_primary_event_count": 5,
        "unmet_status": "BLOCKED",
    }
    for field, expected in expected_readiness.items():
        if readiness.get(field) != expected:
            global_blockers.append(f"readiness_gate_{field}_mismatch")

    metric_units = {
        str(item.get("metric_id")): item.get("unit")
        for item in _sequence(metric_contract.get("metrics"))
        if isinstance(item, Mapping)
    }
    common = [
        _mapping(item) for item in _sequence(policy.get("common_thresholds"))
    ]
    common_ids = {str(item.get("metric_id")) for item in common}
    if common_ids != {
        "return_delta_vs_baseline",
        "max_drawdown_delta_vs_baseline",
        "turnover_delta_vs_baseline",
        "whipsaw_delta",
    }:
        global_blockers.append("common_threshold_metric_set_mismatch")

    candidates = _records_by_key(_sequence(policy.get("candidate_thresholds")), "candidate_id")
    rows: list[dict[str, Any]] = []
    ready_by_candidate: dict[str, bool] = {}
    blockers_by_candidate: dict[str, list[str]] = {}
    for candidate_id in EXPECTED_CANDIDATE_IDS[:2]:
        candidate = _mapping(candidates.get(candidate_id))
        candidate_blockers = list(global_blockers)
        if candidate.get("approval_dependency") != "APPROVED":
            candidate_blockers.append("candidate_thresholds_not_owner_preregistered")
        combined = common + [
            _mapping(item) for item in _sequence(candidate.get("thresholds"))
        ]
        if len(combined) != 6:
            candidate_blockers.append("candidate_threshold_count_mismatch")
        combined_ids = [str(item.get("metric_id")) for item in combined]
        if set(combined_ids) != set(REQUIRED_METRIC_IDS):
            candidate_blockers.append("candidate_threshold_metric_set_mismatch")
        for threshold in combined:
            row_blockers: list[str] = []
            comparator = str(threshold.get("comparator") or "").upper()
            metric_id = str(threshold.get("metric_id") or "")
            if comparator not in SUPPORTED_OPERATORS:
                row_blockers.append(THRESHOLD_OPERATOR_UNSUPPORTED)
            if metric_id not in REQUIRED_METRIC_IDS:
                row_blockers.append(THRESHOLD_METRIC_BINDING_INVALID)
            if not _is_finite_number(threshold.get("value")):
                row_blockers.append(THRESHOLD_VALUE_INVALID)
            if metric_units.get(metric_id) != threshold.get("unit"):
                row_blockers.append("THRESHOLD_METRIC_UNIT_MISMATCH")
            candidate_blockers.extend(row_blockers)
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "threshold_id": threshold.get("threshold_id"),
                    "metric_id": metric_id or None,
                    "operator": comparator or None,
                    "threshold_value": threshold.get("value"),
                    "unit": threshold.get("unit"),
                    "ready": not row_blockers,
                    "gap_codes": sorted(set(row_blockers)),
                }
            )
        candidate_blockers = sorted(set(candidate_blockers))
        blockers_by_candidate[candidate_id] = candidate_blockers
        ready_by_candidate[candidate_id] = not candidate_blockers
    return rows, ready_by_candidate, blockers_by_candidate


def _strict_validation_errors(
    source: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    metric_contract: Mapping[str, Any],
    threshold_policy: Mapping[str, Any],
    source_candidate_ids: Sequence[str],
    review_candidate_ids: Sequence[str],
    review_rows: Sequence[Any],
    documentation_alignment: Mapping[str, bool],
    as_of: str,
) -> list[str]:
    errors: list[str] = []
    expected = (
        (
            source.get("schema_version"),
            EXPECTED_SOURCE_SCHEMA,
            "source_2438m_schema_version_mismatch",
        ),
        (source.get("status"), EXPECTED_SOURCE_STATUS, "source_2438m_status_mismatch"),
        (source.get("next_route"), EXPECTED_SOURCE_ROUTE, "source_2438m_route_mismatch"),
        (
            owner_review.get("schema_version"),
            EXPECTED_OWNER_REVIEW_SCHEMA,
            "owner_review_schema_version_mismatch",
        ),
        (owner_review.get("task_id"), "TRADING-2438M1", "owner_review_task_id_mismatch"),
        (
            owner_review.get("source_task_id"),
            "TRADING-2438M",
            "owner_review_source_task_id_mismatch",
        ),
        (
            owner_review.get("market_regime"),
            "ai_after_chatgpt",
            "owner_review_market_regime_mismatch",
        ),
        (owner_review.get("as_of"), as_of, "owner_review_as_of_mismatch"),
        (
            metric_contract.get("schema_version"),
            EXPECTED_METRIC_CONTRACT_SCHEMA,
            "metric_contract_schema_version_mismatch",
        ),
        (
            threshold_policy.get("schema_version"),
            EXPECTED_THRESHOLD_POLICY_SCHEMA,
            "threshold_policy_schema_version_mismatch",
        ),
    )
    for actual, wanted, code in expected:
        if actual != wanted:
            errors.append(code)
    if source.get("blocked_count") != 3:
        errors.append("source_2438m_blocked_count_mismatch")
    if source.get("candidate_replay_outcome_rechecked") is not True:
        errors.append("source_2438m_outcome_not_rechecked")
    if source.get("as_of") != as_of:
        errors.append("source_2438m_as_of_mismatch")
    if tuple(source_candidate_ids) != EXPECTED_CANDIDATE_IDS:
        errors.append("source_authoritative_candidate_identity_or_order_mismatch")
    row_ids = [
        str(item.get("candidate_id"))
        for item in review_rows
        if isinstance(item, Mapping) and item.get("candidate_id")
    ]
    if source_candidate_ids != review_candidate_ids or source_candidate_ids != row_ids:
        errors.append("candidate_identity_or_order_drift")
    if len(row_ids) != len(set(row_ids)):
        errors.append("duplicate_candidate_id")
    if not owner_review.get("status"):
        errors.append("owner_review_status_missing")
    if not owner_review.get("owner_review_status"):
        errors.append("owner_review_governance_status_missing")
    if not _selection_semantics_ready(_mapping(owner_review.get("candidate_selection"))):
        errors.append("candidate_selection_semantics_invalid")
    if not _safety_boundary_ready(owner_review):
        errors.append("owner_review_safety_boundary_invalid")
    if not all(documentation_alignment.values()):
        errors.append("registry_catalog_docs_alignment_failed")
    for order, review in enumerate(review_rows, start=1):
        if not isinstance(review, Mapping):
            errors.append(f"candidate_review_not_mapping:{order}")
            continue
        if review.get("selection_order") != order:
            errors.append(f"candidate_selection_order_mismatch:{review.get('candidate_id')}")
        if any(key in review for key in ("rank", "source_rank", "performance_rank")):
            errors.append(f"prohibited_performance_rank_field:{review.get('candidate_id')}")
    return sorted(set(errors))


def _overall_status(
    candidate_results: Sequence[Mapping[str, Any]], strict_errors: Sequence[str]
) -> str:
    if strict_errors or not candidate_results:
        return BLOCKED_STATUS
    if any(item.get("decision_complete") is not True for item in candidate_results):
        return BLOCKED_STATUS
    approved = [item for item in candidate_results if item.get("decision") == "APPROVE"]
    if any(item.get("m2_eligible") is not True for item in approved):
        return BLOCKED_STATUS
    return READY_STATUS


def _next_route(status: str, results: Sequence[Mapping[str, Any]]) -> str:
    if status == READY_STATUS:
        if not any(item.get("decision") == "APPROVE" for item in results):
            return NEXT_ROUTE_NO_APPROVED
        return NEXT_ROUTE_READY
    if any(item.get("decision_complete") is not True for item in results):
        return NEXT_ROUTE_DECISION
    return NEXT_ROUTE_BLOCKED


def _source_ready(source: Mapping[str, Any], candidate_ids: Sequence[str]) -> bool:
    return bool(
        source.get("schema_version") == EXPECTED_SOURCE_SCHEMA
        and source.get("status") == EXPECTED_SOURCE_STATUS
        and source.get("next_route") == EXPECTED_SOURCE_ROUTE
        and tuple(candidate_ids) == EXPECTED_CANDIDATE_IDS
        and source.get("blocked_count") == 3
        and source.get("candidate_replay_outcome_rechecked") is True
    )


def _selection_semantics_ready(selection: Mapping[str, Any]) -> bool:
    return bool(
        selection.get("selected_candidate_count") == 3
        and selection.get("selection_basis") == "CONFIG_DECLARATION_ORDER"
        and selection.get("performance_ranked") is False
        and selection.get("pit_evidence_available") is False
    )


def _safety_boundary_ready(owner_review: Mapping[str, Any]) -> bool:
    safety = _mapping(owner_review.get("safety_boundary"))
    return bool(
        safety.get("validation_only") is True
        and safety.get("owner_review_only") is True
        and safety.get("candidate_only") is True
        and safety.get("observe_only") is True
        and safety.get("replay_allowed") is False
        and safety.get("backtest_allowed") is False
        and safety.get("scoring_allowed") is False
        and safety.get("market_data_experiment_allowed") is False
        and safety.get("paper_shadow_allowed") is False
        and safety.get("production_allowed") is False
        and safety.get("production_weight_change_allowed") is False
        and safety.get("broker_action") == "none"
        and safety.get("trading_advice_allowed") is False
    )


def _documentation_alignment(
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> dict[str, bool]:
    report_ids = {
        str(item.get("report_id"))
        for item in _sequence(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    return {
        "report_registry": REPORT_TYPE in report_ids,
        "artifact_catalog": all(
            item in artifact_catalog_text for item in REQUIRED_CATALOG_REFERENCES
        ),
        "system_flow": all(item in system_flow_text for item in REQUIRED_FLOW_REFERENCES),
        "requirement_doc": all(
            item in requirement_text
            for item in (
                "TRADING-2438M1A",
                "TRADING-2438M1B",
                "CONFIG_DECLARATION_ORDER",
                "performance_ranked: false",
                EXPECTED_METRIC_CONTRACT_ID,
                EXPECTED_THRESHOLD_POLICY_ID,
            )
        ),
    }


def _requirements(
    *,
    source_ready: bool,
    candidate_identity_ready: bool,
    selection_semantics_ready: bool,
    safety_ready: bool,
    documentation_ready: bool,
) -> list[dict[str, Any]]:
    checks = (
        ("source_2438m_ready", source_ready),
        ("candidate_identity_and_order_ready", candidate_identity_ready),
        ("candidate_selection_semantics_ready", selection_semantics_ready),
        ("owner_review_safety_boundary_ready", safety_ready),
        ("registry_catalog_docs_alignment", documentation_ready),
    )
    return [
        {
            "requirement_id": requirement_id,
            "status": "PASS" if passed else "FAIL",
            "classification": "owner_input_governance_gap",
        }
        for requirement_id, passed in checks
    ]


def _action(
    candidate_id: str, gap_code: str, field_path: str, recommended_action: str
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "gap_code": gap_code,
        "field_path": field_path,
        "recommended_action": recommended_action,
        "production_effect": "none",
        "broker_action": "none",
    }


def _safety() -> dict[str, Any]:
    return {
        "validation_only": True,
        "owner_review_only": True,
        "candidate_only": True,
        "observe_only": True,
        "replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "market_data_experiment_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "threshold_values_changed": False,
        "candidate_parameters_changed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_weight_change_allowed": False,
        "portfolio_weight_mutated": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _candidate_ids(source: Mapping[str, Any]) -> list[str]:
    for key in ("top3_candidate_ids", "candidate_ids"):
        values = [str(item) for item in _sequence(source.get(key)) if item]
        if values:
            return values
    return [
        str(item.get("candidate_id"))
        for item in _sequence(source.get("candidate_results"))
        if isinstance(item, Mapping) and item.get("candidate_id")
    ]


def _records_by_id(rows: Sequence[Any]) -> dict[str, Mapping[str, Any]]:
    return _records_by_key(rows, "candidate_id")


def _records_by_key(rows: Sequence[Any], key: str) -> dict[str, Mapping[str, Any]]:
    return {
        str(item.get(key)): item
        for item in rows
        if isinstance(item, Mapping) and item.get(key)
    }


def _decision_count(results: Sequence[Mapping[str, Any]], decision: str) -> int:
    return sum(item.get("decision") == decision for item in results)


def _missing_required_fields(
    value: Mapping[str, Any], fields: Sequence[str]
) -> list[str]:
    missing: list[str] = []
    for field in fields:
        item = value.get(field)
        if item is None or item == "" or item == [] or item == {}:
            missing.append(field)
    return missing


def _owner_placeholder_paths(value: Any, prefix: str = "") -> list[str]:
    paths: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            paths.extend(_owner_placeholder_paths(item, path))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for index, item in enumerate(value):
            path = f"{prefix}[{index}]"
            paths.extend(_owner_placeholder_paths(item, path))
    elif isinstance(value, str) and value.startswith("OWNER_MUST_"):
        paths.append(prefix or "owner_placeholder")
    return paths


def _leaf_items(value: Mapping[str, Any], prefix: str = "") -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, Mapping):
            rows.extend(_leaf_items(item, path))
        else:
            rows.append((path, item))
    return rows


def _governed_text(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and value.strip()
        and not value.startswith("OWNER_MUST_")
    )


def _commit_identifier(value: object) -> bool:
    return bool(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value))


def _sha256_identifier(value: object) -> bool:
    return bool(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value))


def _positive_finite(value: object) -> bool:
    return _is_finite_number(value) and float(value) > 0.0


def _is_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
