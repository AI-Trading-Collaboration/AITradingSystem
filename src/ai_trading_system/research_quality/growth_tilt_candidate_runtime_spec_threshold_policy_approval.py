from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

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

SCHEMA_VERSION = "growth_tilt_candidate_runtime_spec_threshold_policy_approval.v1"
CANDIDATE_MATRIX_SCHEMA_VERSION = "growth_tilt_candidate_runtime_spec_review_matrix.v1"
METRIC_MATRIX_SCHEMA_VERSION = "growth_tilt_metric_contract_review_matrix.v1"
THRESHOLD_MATRIX_SCHEMA_VERSION = "growth_tilt_threshold_policy_review_matrix.v1"
OWNER_CHECKLIST_SCHEMA_VERSION = "growth_tilt_runtime_spec_owner_action_checklist.v1"
NO_EFFECT_SCHEMA_VERSION = "growth_tilt_runtime_spec_threshold_approval_no_effect.v1"

REPORT_TYPE = "growth_tilt_candidate_runtime_spec_threshold_policy_approval"
READY_STATUS = "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_"
    "BLOCKED_OWNER_INPUT"
)
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
    "growth_tilt_candidate_runtime_spec_threshold_policy_review.v1"
)

NEXT_ROUTE_READY = "TRADING-2438M2_Growth_Tilt_Candidate_Runtime_Compute_Plane_Binding"
NEXT_ROUTE_BLOCKED = EXPECTED_SOURCE_ROUTE
NEXT_ROUTE_REDEFINE = "TRADING-2438M1A_Growth_Tilt_Candidate_Definition_Design"
NEXT_ROUTE_WITHDRAW = "TRADING-2438M1A_Growth_Tilt_Top3_Candidate_Reselection"

ALLOWED_DECISIONS = {"PENDING", "APPROVE", "REDEFINE", "WITHDRAW"}
REQUIRED_REVIEW_FIELDS = (
    "decision_rationale",
    "review_owner",
    "reviewed_at",
    "review_condition",
    "expiry_condition",
)
REQUIRED_RUNTIME_SPEC_FIELDS = (
    "executor_id",
    "executor_version",
    "input_contract_version",
    "source_policy_ref",
)
REQUIRED_METRIC_SPEC_FIELDS = (
    "source_field",
    "unit",
    "normalization_rule_id",
    "calculator_id",
    "calculator_version",
)
REQUIRED_THRESHOLD_FIELDS = (
    "threshold_id",
    "metric_id",
    "operator",
    "policy_owner",
    "policy_version",
    "policy_status",
    "rationale",
    "evaluator_id",
    "evaluator_version",
    "review_condition",
    "expiry_condition",
)

DECISION_PENDING = "OWNER_CANDIDATE_DECISION_PENDING"
DECISION_INVALID = "OWNER_CANDIDATE_DECISION_INVALID"
REVIEW_METADATA_INCOMPLETE = "OWNER_CANDIDATE_REVIEW_METADATA_INCOMPLETE"
RUNTIME_SPEC_INCOMPLETE = "OWNER_RUNTIME_SPEC_INCOMPLETE"
METRIC_CONTRACT_INCOMPLETE = "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE"
THRESHOLD_POLICY_INCOMPLETE = "OWNER_THRESHOLD_POLICY_INCOMPLETE"
THRESHOLD_VALUE_INVALID = "OWNER_THRESHOLD_VALUE_INVALID"
THRESHOLD_OPERATOR_UNSUPPORTED = "OWNER_THRESHOLD_OPERATOR_UNSUPPORTED"
THRESHOLD_METRIC_BINDING_INVALID = "OWNER_THRESHOLD_METRIC_BINDING_INVALID"
CANDIDATE_REDEFINITION_REQUIRED = "OWNER_CANDIDATE_REDEFINITION_REQUIRED"
CANDIDATE_WITHDRAWAL_REQUIRED = "OWNER_CANDIDATE_WITHDRAWAL_RESELECTION_REQUIRED"
SAFETY_BOUNDARY_INVALID = "OWNER_REVIEW_SAFETY_BOUNDARY_INVALID"

REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "growth-tilt-candidate-runtime-spec-threshold-policy-approval",
    "approval_readiness_result.json",
    "candidate_runtime_spec_review_matrix.json",
    "metric_contract_review_matrix.json",
    "threshold_policy_review_matrix.json",
    "owner_action_checklist.json",
    "no_effect_boundary.json",
)
REQUIRED_FLOW_REFERENCES: tuple[str, ...] = (
    "TRADING-2438M1",
    READY_STATUS,
    BLOCKED_STATUS,
    REDEFINE_STATUS,
    WITHDRAW_STATUS,
    NEXT_ROUTE_READY,
)


def build_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
    source_2438m: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    *,
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    as_of: str,
) -> dict[str, Any]:
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
    strict_errors = _strict_validation_errors(
        source_2438m,
        owner_review,
        source_candidate_ids,
        review_candidate_ids,
        review_rows,
        documentation_alignment,
        as_of,
    )

    candidate_results: list[dict[str, Any]] = []
    metric_matrix: list[dict[str, Any]] = []
    threshold_matrix: list[dict[str, Any]] = []
    owner_actions: list[dict[str, Any]] = []
    for source_rank, candidate_id in enumerate(source_candidate_ids, start=1):
        result, metric_rows, threshold_rows, actions = _review_candidate(
            candidate_id,
            source_rank,
            reviews_by_id.get(candidate_id, {}),
        )
        candidate_results.append(result)
        metric_matrix.extend(metric_rows)
        threshold_matrix.extend(threshold_rows)
        owner_actions.extend(actions)

    status = _overall_status(candidate_results, strict_errors)
    next_route = _next_route(status)
    decision_counts = Counter(
        str(item.get("decision")) for item in candidate_results if item.get("decision")
    )
    gap_counts = Counter(str(item.get("gap_code")) for item in owner_actions)
    source_ready = _source_ready(source_2438m, source_candidate_ids)
    requirements = _requirements(
        source_ready=source_ready,
        candidate_identity_ready=(
            source_candidate_ids == review_candidate_ids
            and tuple(source_candidate_ids) == EXPECTED_CANDIDATE_IDS
            and len(source_candidate_ids) == len(reviews_by_id) == 3
        ),
        safety_ready=_safety_boundary_ready(owner_review),
        documentation_ready=all(documentation_alignment.values()),
    )
    gaps = [item for item in requirements if item["status"] != "PASS"]
    safety = _safety()

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M1",
        "report_type": REPORT_TYPE,
        "status": status,
        "readiness_status": status,
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "source_status": source_2438m.get("status"),
        "source_run_id": source_2438m.get("run_id"),
        "source_artifacts": [dict(item) for item in source_artifacts],
        "source_2438m_ready_for_owner_review": source_ready,
        "candidate_count": len(candidate_results),
        "candidate_ids": source_candidate_ids,
        "owner_review_status": owner_review.get("owner_review_status"),
        "owner_decision_counts": dict(sorted(decision_counts.items())),
        "approved_candidate_count": _review_status_count(candidate_results, "APPROVED"),
        "pending_candidate_count": _decision_count(candidate_results, "PENDING"),
        "redefine_candidate_count": _decision_count(candidate_results, "REDEFINE"),
        "withdraw_candidate_count": _decision_count(candidate_results, "WITHDRAW"),
        "runtime_spec_ready_count": sum(
            item.get("runtime_spec_ready") is True for item in candidate_results
        ),
        "metric_contract_ready_count": sum(
            item.get("metric_contract_ready") is True for item in candidate_results
        ),
        "threshold_policy_ready_count": sum(
            item.get("threshold_policy_ready") is True for item in candidate_results
        ),
        "owner_input_gap_count": len(owner_actions),
        "owner_input_gaps_by_code": dict(sorted(gap_counts.items())),
        "candidate_reviews": candidate_results,
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
            "required_metric_ids": list(REQUIRED_METRIC_IDS),
            "rows": metric_matrix,
        },
        "threshold_policy_review_matrix": {
            "schema_version": THRESHOLD_MATRIX_SCHEMA_VERSION,
            "status": status,
            "supported_operators": sorted(SUPPORTED_OPERATORS),
            "rows": threshold_matrix,
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
            "M1 reads prior validated 2438M evidence, governance config, and owner "
            "review input only; it does not read cached market or outcome data."
        ),
        **safety,
    }


def _review_candidate(
    candidate_id: str,
    source_rank: int,
    review: Mapping[str, Any],
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    decision = str(review.get("decision") or "PENDING").upper()
    actions: list[dict[str, Any]] = []
    if decision not in ALLOWED_DECISIONS:
        actions.append(
            _action(
                candidate_id,
                DECISION_INVALID,
                "decision",
                f"Choose one of {sorted(ALLOWED_DECISIONS)}.",
            )
        )
    elif decision == "PENDING":
        actions.append(
            _action(
                candidate_id,
                DECISION_PENDING,
                "decision",
                "Record an explicit APPROVE, REDEFINE, or WITHDRAW owner decision.",
            )
        )
    elif decision in {"REDEFINE", "WITHDRAW"}:
        missing = [
            field
            for field in ("decision_rationale", "review_owner", "reviewed_at", "next_route")
            if not review.get(field)
        ]
        if missing:
            actions.append(
                _action(
                    candidate_id,
                    REVIEW_METADATA_INCOMPLETE,
                    "candidate_review",
                    f"Complete decision metadata: {', '.join(missing)}.",
                )
            )
        actions.append(
            _action(
                candidate_id,
                CANDIDATE_REDEFINITION_REQUIRED
                if decision == "REDEFINE"
                else CANDIDATE_WITHDRAWAL_REQUIRED,
                "decision",
                "Follow the explicit candidate definition or reselection route.",
            )
        )

    runtime_spec = _mapping(review.get("runtime_spec"))
    runtime_spec_ready, runtime_missing = _runtime_spec_status(runtime_spec)
    metric_rows, metric_contract_ready = _metric_review_rows(candidate_id, review)
    threshold_rows, threshold_policy_ready = _threshold_review_rows(
        candidate_id, review
    )
    review_metadata_ready = all(review.get(field) for field in REQUIRED_REVIEW_FIELDS)

    if decision == "APPROVE":
        if not review_metadata_ready:
            missing = [field for field in REQUIRED_REVIEW_FIELDS if not review.get(field)]
            actions.append(
                _action(
                    candidate_id,
                    REVIEW_METADATA_INCOMPLETE,
                    "candidate_review",
                    f"Complete owner review metadata: {', '.join(missing)}.",
                )
            )
        if not runtime_spec_ready:
            actions.append(
                _action(
                    candidate_id,
                    RUNTIME_SPEC_INCOMPLETE,
                    "runtime_spec",
                    f"Complete approved runtime spec fields: {', '.join(runtime_missing)}.",
                )
            )
        if not metric_contract_ready:
            actions.extend(_metric_actions(candidate_id, metric_rows))
        if not threshold_policy_ready:
            actions.extend(_threshold_actions(candidate_id, threshold_rows))
    elif decision == "PENDING":
        if not runtime_spec_ready:
            actions.append(
                _action(
                    candidate_id,
                    RUNTIME_SPEC_INCOMPLETE,
                    "runtime_spec",
                    "Provide the executable candidate parameter contract if approving.",
                )
            )
        if not metric_contract_ready:
            actions.extend(_metric_actions(candidate_id, metric_rows))
        if not threshold_policy_ready:
            actions.extend(_threshold_actions(candidate_id, threshold_rows))

    approval_ready = bool(
        decision == "APPROVE"
        and review_metadata_ready
        and runtime_spec_ready
        and metric_contract_ready
        and threshold_policy_ready
    )
    review_status = (
        "APPROVED"
        if approval_ready
        else (
            "REDEFINITION_REQUIRED"
            if decision == "REDEFINE"
            else (
                "WITHDRAWAL_RESELECTION_REQUIRED"
                if decision == "WITHDRAW"
                else ("PENDING" if decision == "PENDING" else "APPROVAL_INCOMPLETE")
            )
        )
    )
    return (
        {
            "candidate_id": candidate_id,
            "source_rank": source_rank,
            "decision": decision,
            "review_status": review_status,
            "decision_rationale": review.get("decision_rationale"),
            "review_owner": review.get("review_owner"),
            "reviewed_at": review.get("reviewed_at"),
            "next_route": review.get("next_route"),
            "review_metadata_ready": review_metadata_ready,
            "runtime_spec_ready": runtime_spec_ready,
            "metric_contract_ready": metric_contract_ready,
            "threshold_policy_ready": threshold_policy_ready,
            "approval_ready": approval_ready,
            "gap_codes": sorted(
                {
                    str(item.get("gap_code"))
                    for item in actions
                    if item.get("candidate_id") == candidate_id
                }
            ),
        },
        metric_rows,
        threshold_rows,
        actions,
    )


def _runtime_spec_status(spec: Mapping[str, Any]) -> tuple[bool, list[str]]:
    missing = [field for field in REQUIRED_RUNTIME_SPEC_FIELDS if not spec.get(field)]
    if spec.get("approved") is not True:
        missing.append("approved=true")
    parameters = spec.get("parameters")
    if not isinstance(parameters, Mapping) or not parameters:
        missing.append("non_empty_parameters")
    return not missing, missing


def _metric_review_rows(
    candidate_id: str, review: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], bool]:
    specs = _sequence(review.get("metric_specs"))
    by_id = _records_by_key(specs, "metric_id")
    rows: list[dict[str, Any]] = []
    for metric_id in REQUIRED_METRIC_IDS:
        spec = _mapping(by_id.get(metric_id))
        missing = [field for field in REQUIRED_METRIC_SPEC_FIELDS if not spec.get(field)]
        rows.append(
            {
                "candidate_id": candidate_id,
                "metric_id": metric_id,
                "source_field": spec.get("source_field"),
                "unit": spec.get("unit"),
                "normalization_rule_id": spec.get("normalization_rule_id"),
                "calculator_id": spec.get("calculator_id"),
                "calculator_version": spec.get("calculator_version"),
                "ready": not missing,
                "missing_fields": missing,
            }
        )
    return rows, bool(rows) and all(item["ready"] is True for item in rows)


def _threshold_review_rows(
    candidate_id: str, review: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], bool]:
    specs = _sequence(review.get("threshold_specs"))
    if not specs:
        return (
            [
                {
                    "candidate_id": candidate_id,
                    "threshold_id": None,
                    "metric_id": None,
                    "operator": None,
                    "threshold_value": None,
                    "ready": False,
                    "gap_codes": [THRESHOLD_POLICY_INCOMPLETE],
                }
            ],
            False,
        )
    rows: list[dict[str, Any]] = []
    for raw in specs:
        spec = _mapping(raw)
        operator = str(spec.get("operator") or "").upper()
        metric_id = str(spec.get("metric_id") or "")
        missing = [field for field in REQUIRED_THRESHOLD_FIELDS if not spec.get(field)]
        gap_codes: list[str] = []
        if missing:
            gap_codes.append(THRESHOLD_POLICY_INCOMPLETE)
        if operator not in SUPPORTED_OPERATORS:
            gap_codes.append(THRESHOLD_OPERATOR_UNSUPPORTED)
        if metric_id not in REQUIRED_METRIC_IDS:
            gap_codes.append(THRESHOLD_METRIC_BINDING_INVALID)
        if not _threshold_value_ready(operator, spec.get("threshold_value")):
            gap_codes.append(THRESHOLD_VALUE_INVALID)
        evidence = _sequence(spec.get("validation_evidence"))
        if not evidence:
            gap_codes.append(THRESHOLD_POLICY_INCOMPLETE)
        if spec.get("policy_status") != "APPROVED":
            gap_codes.append(THRESHOLD_POLICY_INCOMPLETE)
        rows.append(
            {
                "candidate_id": candidate_id,
                "threshold_id": spec.get("threshold_id"),
                "metric_id": metric_id or None,
                "operator": operator or None,
                "threshold_value": spec.get("threshold_value"),
                "policy_owner": spec.get("policy_owner"),
                "policy_version": spec.get("policy_version"),
                "policy_status": spec.get("policy_status"),
                "evaluator_id": spec.get("evaluator_id"),
                "evaluator_version": spec.get("evaluator_version"),
                "validation_evidence": evidence,
                "ready": not gap_codes,
                "missing_fields": missing,
                "gap_codes": sorted(set(gap_codes)),
            }
        )
    return rows, all(item["ready"] is True for item in rows)


def _strict_validation_errors(
    source: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    source_candidate_ids: Sequence[str],
    review_candidate_ids: Sequence[str],
    review_rows: Sequence[Any],
    documentation_alignment: Mapping[str, bool],
    as_of: str,
) -> list[str]:
    errors: list[str] = []
    if source.get("schema_version") != EXPECTED_SOURCE_SCHEMA:
        errors.append("source_2438m_schema_version_mismatch")
    if source.get("status") != EXPECTED_SOURCE_STATUS:
        errors.append("source_2438m_status_mismatch")
    if source.get("next_route") != EXPECTED_SOURCE_ROUTE:
        errors.append("source_2438m_route_mismatch")
    if source.get("blocked_count") != 3:
        errors.append("source_2438m_blocked_count_mismatch")
    if source.get("candidate_replay_outcome_rechecked") is not True:
        errors.append("source_2438m_outcome_not_rechecked")
    if source.get("as_of") != as_of:
        errors.append("source_2438m_as_of_mismatch")
    if tuple(source_candidate_ids) != EXPECTED_CANDIDATE_IDS:
        errors.append("source_authoritative_candidate_identity_or_order_mismatch")
    if owner_review.get("schema_version") != EXPECTED_OWNER_REVIEW_SCHEMA:
        errors.append("owner_review_schema_version_mismatch")
    if owner_review.get("task_id") != "TRADING-2438M1":
        errors.append("owner_review_task_id_mismatch")
    if owner_review.get("source_task_id") != "TRADING-2438M":
        errors.append("owner_review_source_task_id_mismatch")
    if owner_review.get("market_regime") != "ai_after_chatgpt":
        errors.append("owner_review_market_regime_mismatch")
    if owner_review.get("as_of") != as_of:
        errors.append("owner_review_as_of_mismatch")
    if not owner_review.get("status"):
        errors.append("owner_review_status_missing")
    if not owner_review.get("owner_review_status"):
        errors.append("owner_review_governance_status_missing")
    row_ids = [
        str(item.get("candidate_id"))
        for item in review_rows
        if isinstance(item, Mapping) and item.get("candidate_id")
    ]
    if source_candidate_ids != review_candidate_ids or source_candidate_ids != row_ids:
        errors.append("candidate_identity_or_order_drift")
    if len(row_ids) != len(set(row_ids)):
        errors.append("duplicate_candidate_id")
    if len(source_candidate_ids) != 3:
        errors.append("source_top3_candidate_count_mismatch")
    if not _safety_boundary_ready(owner_review):
        errors.append("owner_review_safety_boundary_invalid")
    if not all(documentation_alignment.values()):
        errors.append("registry_catalog_docs_alignment_failed")
    for review in review_rows:
        if not isinstance(review, Mapping):
            continue
        metric_ids = [
            str(item.get("metric_id"))
            for item in _sequence(review.get("metric_specs"))
            if isinstance(item, Mapping) and item.get("metric_id")
        ]
        if len(metric_ids) != len(set(metric_ids)):
            errors.append(f"duplicate_metric_id:{review.get('candidate_id')}")
        unknown_metrics = sorted(set(metric_ids) - set(REQUIRED_METRIC_IDS))
        if unknown_metrics:
            errors.append(
                f"unknown_metric_id:{review.get('candidate_id')}:{','.join(unknown_metrics)}"
            )
        threshold_ids: list[str] = []
        for threshold in _sequence(review.get("threshold_specs")):
            if not isinstance(threshold, Mapping):
                continue
            if threshold.get("threshold_id"):
                threshold_ids.append(str(threshold.get("threshold_id")))
            operator = str(threshold.get("operator") or "").upper()
            if operator and operator not in SUPPORTED_OPERATORS:
                errors.append(
                    f"unsupported_threshold_operator:{review.get('candidate_id')}:{operator}"
                )
            metric_id = str(threshold.get("metric_id") or "")
            if metric_id and metric_id not in REQUIRED_METRIC_IDS:
                errors.append(
                    f"unknown_threshold_metric:{review.get('candidate_id')}:{metric_id}"
                )
        if len(threshold_ids) != len(set(threshold_ids)):
            errors.append(f"duplicate_threshold_id:{review.get('candidate_id')}")
    return errors


def _source_ready(source: Mapping[str, Any], candidate_ids: Sequence[str]) -> bool:
    return bool(
        source.get("schema_version") == EXPECTED_SOURCE_SCHEMA
        and source.get("status") == EXPECTED_SOURCE_STATUS
        and source.get("next_route") == EXPECTED_SOURCE_ROUTE
        and tuple(candidate_ids) == EXPECTED_CANDIDATE_IDS
        and source.get("blocked_count") == 3
        and source.get("candidate_replay_outcome_rechecked") is True
    )


def _overall_status(
    candidate_results: Sequence[Mapping[str, Any]], strict_errors: Sequence[str]
) -> str:
    decisions = {str(item.get("decision")) for item in candidate_results}
    if "WITHDRAW" in decisions:
        return WITHDRAW_STATUS
    if "REDEFINE" in decisions:
        return REDEFINE_STATUS
    if (
        candidate_results
        and not strict_errors
        and all(item.get("approval_ready") is True for item in candidate_results)
    ):
        return READY_STATUS
    return BLOCKED_STATUS


def _next_route(status: str) -> str:
    if status == READY_STATUS:
        return NEXT_ROUTE_READY
    if status == REDEFINE_STATUS:
        return NEXT_ROUTE_REDEFINE
    if status == WITHDRAW_STATUS:
        return NEXT_ROUTE_WITHDRAW
    return NEXT_ROUTE_BLOCKED


def _threshold_value_ready(operator: str, value: object) -> bool:
    if operator in {"BETWEEN", "OUTSIDE"}:
        bounds = _sequence(value)
        return len(bounds) == 2 and all(_is_finite_number(item) for item in bounds)
    return _is_finite_number(value)


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
        "requirement_doc": "TRADING-2438M1" in requirement_text
        and "APPROVE" in requirement_text
        and "WITHDRAW" in requirement_text,
    }


def _requirements(
    *,
    source_ready: bool,
    candidate_identity_ready: bool,
    safety_ready: bool,
    documentation_ready: bool,
) -> list[dict[str, Any]]:
    checks = (
        ("source_2438m_ready", source_ready),
        ("candidate_identity_and_order_ready", candidate_identity_ready),
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


def _metric_actions(
    candidate_id: str, metric_rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    return [
        _action(
            candidate_id,
            METRIC_CONTRACT_INCOMPLETE,
            f"metric_specs.{row.get('metric_id')}",
            "Complete metric contract fields: "
            f"{', '.join(str(item) for item in _sequence(row.get('missing_fields')))}.",
        )
        for row in metric_rows
        if row.get("ready") is not True
    ]


def _threshold_actions(
    candidate_id: str, threshold_rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for row in threshold_rows:
        if row.get("ready") is True:
            continue
        threshold_id = row.get("threshold_id") or "missing_threshold_spec"
        for gap_code in _sequence(row.get("gap_codes")) or [
            THRESHOLD_POLICY_INCOMPLETE
        ]:
            actions.append(
                _action(
                    candidate_id,
                    str(gap_code),
                    f"threshold_specs.{threshold_id}",
                    "Complete the governed threshold record without deriving a value "
                    "from current metrics or prior runs.",
                )
            )
    return actions


def _candidate_ids(source: Mapping[str, Any]) -> list[str]:
    values = source.get("top3_candidate_ids") or source.get("candidate_ids")
    return [str(item) for item in _sequence(values) if item]


def _records_by_id(records: Sequence[Any]) -> dict[str, Mapping[str, Any]]:
    return _records_by_key(records, "candidate_id")


def _records_by_key(
    records: Sequence[Any], key: str
) -> dict[str, Mapping[str, Any]]:
    return {
        str(item.get(key)): item
        for item in records
        if isinstance(item, Mapping) and item.get(key)
    }


def _review_status_count(results: Sequence[Mapping[str, Any]], status: str) -> int:
    return sum(item.get("review_status") == status for item in results)


def _decision_count(results: Sequence[Mapping[str, Any]], decision: str) -> int:
    return sum(item.get("decision") == decision for item in results)


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


def _is_finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
