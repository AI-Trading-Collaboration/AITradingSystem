# Growth Tilt Candidate Family Closure

当前 A/B/C/replacement-A family 已正式关闭为 completed negative research evidence。关闭不是 FAIL 或实现失败；它表示没有 approved、contract-complete、PIT-executable candidate。

```json
{
  "as_of": "2026-07-10",
  "baseline_adapter_blocked_count": 4,
  "baseline_adapter_ready_count": 0,
  "candidate_dispositions": {
    "capped_recovery_permission_overlay": "KEEP_REDEFINED_BLOCKED",
    "false_risk_off_confirmation_relaxation": "WITHDRAW",
    "missed_upside_reentry_accelerator": "REDEFINE",
    "recovery_reentry_speedup_guard": "REDEFINE"
  },
  "closure_reason_codes": [
    "NO_APPROVED_CANDIDATE",
    "BASELINE_ADAPTERS_NOT_RUNTIME_READY",
    "REPLACEMENT_CANDIDATE_PREREQUISITES_BLOCKED",
    "NO_REAL_PIT_REPLAY_EXECUTED"
  ],
  "closure_status": "CLOSED_NO_EXECUTABLE_PIT_CANDIDATE",
  "family_id": "growth_tilt_false_risk_off_missed_upside_2433",
  "next_route": "TRADING-2438N2_GROWTH_TILT_BASELINE_CAPABILITY_GRAPH",
  "pit_candidates_tested": 0,
  "prerequisite_blocked_count": 8,
  "prerequisite_pass_count": 2,
  "status": "GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE"
}
```

## Exact M1E prerequisite matrix

```json
{
  "all_prerequisites_ready": false,
  "blocked_count": 8,
  "blocker_codes": [
    "RECOVERY_PRODUCER_PIT_LINEAGE_VALID",
    "RECOVERY_THRESHOLD_VERSIONED_AND_PREREGISTERED",
    "HARD_VETO_AGGREGATE_CONTRACT_READY",
    "CURRENT_REQUESTED_APPLIED_TRANSITION_CONTRACT_READY",
    "NATIVE_EXPOSURE_SCALAR_CONTRACT_READY",
    "CANDIDATE_CAP_EXPRESSED_IN_NATIVE_UNITS",
    "SCREENING_POLICY_FROZEN_BEFORE_RESULT_VISIBILITY",
    "SECOND_OWNER_APPROVAL_RECORDED"
  ],
  "pass_count": 2,
  "prerequisite_count": 10,
  "replacement_candidate_id": "capped_recovery_permission_overlay",
  "rows": [
    {
      "blocker_code": null,
      "prerequisite_id": "replacement_identity_and_orthogonal_role_frozen",
      "ready": true,
      "status": "PASS"
    },
    {
      "blocker_code": null,
      "prerequisite_id": "recovery_output_semantic_type_known",
      "ready": true,
      "status": "PASS"
    },
    {
      "blocker_code": "RECOVERY_PRODUCER_PIT_LINEAGE_VALID",
      "prerequisite_id": "recovery_producer_pit_lineage_valid",
      "ready": false,
      "status": "BLOCKED"
    },
    {
      "blocker_code": "RECOVERY_THRESHOLD_VERSIONED_AND_PREREGISTERED",
      "prerequisite_id": "recovery_threshold_versioned_and_preregistered",
      "ready": false,
      "status": "BLOCKED"
    },
    {
      "blocker_code": "HARD_VETO_AGGREGATE_CONTRACT_READY",
      "prerequisite_id": "hard_veto_aggregate_contract_ready",
      "ready": false,
      "status": "BLOCKED"
    },
    {
      "blocker_code": "CURRENT_REQUESTED_APPLIED_TRANSITION_CONTRACT_READY",
      "prerequisite_id": "current_requested_applied_transition_contract_ready",
      "ready": false,
      "status": "BLOCKED"
    },
    {
      "blocker_code": "NATIVE_EXPOSURE_SCALAR_CONTRACT_READY",
      "prerequisite_id": "native_exposure_scalar_contract_ready",
      "ready": false,
      "status": "BLOCKED"
    },
    {
      "blocker_code": "CANDIDATE_CAP_EXPRESSED_IN_NATIVE_UNITS",
      "prerequisite_id": "candidate_cap_expressed_in_native_units",
      "ready": false,
      "status": "BLOCKED"
    },
    {
      "blocker_code": "SCREENING_POLICY_FROZEN_BEFORE_RESULT_VISIBILITY",
      "prerequisite_id": "screening_policy_frozen_before_result_visibility",
      "ready": false,
      "status": "BLOCKED"
    },
    {
      "blocker_code": "SECOND_OWNER_APPROVAL_RECORDED",
      "prerequisite_id": "second_owner_approval_recorded",
      "ready": false,
      "status": "BLOCKED"
    }
  ],
  "schema_version": "growth_tilt_replacement_candidate_prerequisite_matrix.v1"
}
```

## Negative-result ledger

```json
{
  "configuration_order_is_performance_rank": false,
  "family_id": "growth_tilt_false_risk_off_missed_upside_2433",
  "new_baseline_behavior_allowed": false,
  "record_count": 4,
  "records": [
    {
      "hypothesis_id": "recovery_reentry_speedup_guard",
      "intended_failure_target": "SLOW_GROWTH_RECOVERY_REENTRY",
      "missing_baseline_capabilities": [
        "RECOVERY_PRODUCER_PIT_LINEAGE",
        "BASELINE_RECOVERY_TRANSITION_CONSUMPTION",
        "BASELINE_RECOVERY_PERSISTENCE"
      ],
      "non_executability_reason": "NO_GOVERNED_BASELINE_RECOVERY_CONSUMPTION_RULE",
      "original_candidate_role": "RECOVERY_REENTRY_TIMING_ACCELERATOR",
      "prohibited_future_reuse": [
        "DO_NOT_DESCRIBE_AS_BASELINE_SPEEDUP",
        "DO_NOT_CREATE_BASELINE_PERSISTENCE_TO_RESCUE_CANDIDATE"
      ],
      "reopen_condition": "INDEPENDENT_BASELINE_RECOVERY_CONSUMPTION_PROJECT",
      "research_design_lesson": [
        "a callable producer does not imply baseline consumption",
        "a candidate cannot be a delta from a nonexistent baseline rule"
      ],
      "terminal_disposition": "REDEFINE"
    },
    {
      "hypothesis_id": "false_risk_off_confirmation_relaxation",
      "intended_failure_target": "FALSE_RISK_OFF_ENTRY",
      "missing_baseline_capabilities": [
        "CALLABLE_PIT_SOFT_CONFIRMATION",
        "CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST"
      ],
      "non_executability_reason": "NO_CALLABLE_SOFT_CONFIRMATION_OR_AGGREGATE_NON_HARD_REQUEST",
      "original_candidate_role": "DEFENSIVE_ENTRY_SOFT_CONFIRMATION_GRACE",
      "prohibited_future_reuse": [
        "DO_NOT_INFER_SOFT_CONFIRMATION_FROM_CONCEPTUAL_LABEL",
        "DO_NOT_ADD_AGGREGATE_REQUEST_INSIDE_CANDIDATE_TASK"
      ],
      "reopen_condition": "INDEPENDENT_NON_HARD_DEFENSIVE_REQUEST_PROJECT",
      "research_design_lesson": [
        "soft confirmation cannot be inferred from a conceptual label"
      ],
      "terminal_disposition": "WITHDRAW"
    },
    {
      "hypothesis_id": "missed_upside_reentry_accelerator",
      "intended_failure_target": "MISSED_UPSIDE_AFTER_RECOVERY",
      "missing_baseline_capabilities": [
        "READY_RAMP_RULE",
        "GOVERNED_NATIVE_EXPOSURE_SCALAR",
        "GOVERNED_REQUESTED_APPLIED_TRANSITION"
      ],
      "non_executability_reason": "NON_ORTHOGONAL_AND_NO_INDEPENDENT_EXECUTABLE_RAMP_CONTRACT",
      "original_candidate_role": "RECOVERY_REENTRY_ACCELERATOR",
      "prohibited_future_reuse": [
        "DO_NOT_DUPLICATE_TRIGGER_TIMING_AXIS",
        "DO_NOT_CALL_DECLARATION_ORDER_A_PERFORMANCE_RANK"
      ],
      "reopen_condition": "INDEPENDENT_POST_CONFIRMATION_RAMP_CONTRACT",
      "research_design_lesson": [
        "config declaration order is not performance ranking",
        "structural orthogonality must precede candidate selection"
      ],
      "terminal_disposition": "REDEFINE"
    },
    {
      "hypothesis_id": "capped_recovery_permission_overlay",
      "intended_failure_target": "MISSED_UPSIDE_WITH_CAPPED_RECOVERY_PERMISSION",
      "missing_baseline_capabilities": [
        "RECOVERY_PRODUCER_PIT_LINEAGE_VALID",
        "RECOVERY_THRESHOLD_VERSIONED_AND_PREREGISTERED",
        "HARD_VETO_AGGREGATE_CONTRACT_READY",
        "CURRENT_REQUESTED_APPLIED_TRANSITION_CONTRACT_READY",
        "NATIVE_EXPOSURE_SCALAR_CONTRACT_READY",
        "CANDIDATE_CAP_EXPRESSED_IN_NATIVE_UNITS",
        "SCREENING_POLICY_FROZEN_BEFORE_RESULT_VISIBILITY",
        "SECOND_OWNER_APPROVAL_RECORDED"
      ],
      "non_executability_reason": "M1E_APPROVAL_PREREQUISITES_BLOCKED",
      "original_candidate_role": "CAPPED_RECOVERY_PERMISSION_OVERLAY",
      "prohibited_future_reuse": [
        "DO_NOT_REUSE_QQQ_EQUIVALENT_AS_CANDIDATE_DELTA_UNIT",
        "DO_NOT_TREAT_PARTIAL_HARD_VETO_SET_AS_REPLAY_DETAIL",
        "DO_NOT_APPROVE_WITH_DEFAULT_OR_POST_REPLAY_THRESHOLD"
      ],
      "reopen_condition": "ALL_M1E_PREREQUISITES_PROVEN_BY_INDEPENDENT_BASELINE_WORK",
      "research_design_lesson": [
        "candidate delta units must come from governed exposure semantics",
        "hard-veto completeness is an admission prerequisite, not a late replay detail"
      ],
      "terminal_disposition": "KEEP_REDEFINED_BLOCKED"
    }
  ],
  "required_research_design_lessons": [
    "config declaration order is not performance ranking",
    "a callable producer does not imply baseline consumption",
    "a candidate cannot be a delta from a nonexistent baseline rule",
    "soft confirmation cannot be inferred from a conceptual label",
    "candidate delta units must come from governed exposure semantics",
    "hard-veto completeness is an admission prerequisite, not a late replay detail"
  ],
  "schema_version": "growth_tilt_candidate_negative_result_ledger.v1"
}
```

## Reopen policy

```json
{
  "allowed_new_evidence_types": [
    "RECOVERY_PERMISSION_PIT_GOVERNED_PRODUCER",
    "BASELINE_RECOVERY_TRANSITION_CONSUMPTION_PATH",
    "RUNTIME_READY_AUTHORITATIVE_HARD_VETO_AGGREGATE",
    "RUNTIME_READY_GOVERNED_NATIVE_EXPOSURE_SCALAR",
    "INDEPENDENT_CALLABLE_NON_HARD_DEFENSIVE_REQUEST"
  ],
  "baseline_change_motivation": null,
  "baseline_change_task_ids": [],
  "candidate_independent_change": true,
  "candidate_work_may_motivate_baseline_change": false,
  "family_id": "growth_tilt_false_risk_off_missed_upside_2433",
  "minimum_independent_evidence_type_count": 1,
  "new_evidence_refs": [],
  "owner_reapproval_required": true,
  "reopen_ready": false,
  "schema_version": "growth_tilt_candidate_family_reopen_policy.v1",
  "screening_policy_refreeze_required": true,
  "status": "CLOSED_PENDING_INDEPENDENT_BASELINE_EVIDENCE"
}
```

## 结论

旧 family 的 M2 route 已关闭。只有 candidate-independent baseline work产生受治理的新 capability evidence并重新 owner approval/refreeze policy后，才可新开 reopen task；当前下一步是 read-only TRADING-2438N2 capability graph。
