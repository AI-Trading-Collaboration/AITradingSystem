# Growth Tilt Owner Decision Resolution

D01～D18 已全部决议；`RESOLVED_BLOCKED` 是明确的 owner outcome，不是缺失输入。A 改为 replacement overlay proposal，B WITHDRAW，C 保留 REDEFINE/out-of-route。该报告不运行 runtime code 或 replay。

```json
{
  "approved_candidate_count": 0,
  "as_of": "2026-07-10",
  "blocking_decision_ids": [
    "D02",
    "D04",
    "D11",
    "D12",
    "D13",
    "D15",
    "D18"
  ],
  "decision_count": 18,
  "m1d2_adapter_implementation_allowed": true,
  "m2_eligible_candidate_count": 0,
  "next_route": "TRADING-2438M1D2_GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_AND_READINESS",
  "owner_decisions_complete": true,
  "redefine_candidate_count": 2,
  "resolved_decision_count": 18,
  "status": "GROWTH_TILT_OWNER_DECISION_RESOLUTION_READY_WITH_BLOCKERS",
  "strict_validation_error_count": 0,
  "withdraw_candidate_count": 1
}
```

## Candidate Disposition

```json
{
  "approved_candidate_count": 0,
  "candidates": [
    {
      "candidate_id": "recovery_reentry_speedup_guard",
      "current_route_enabled": true,
      "decision": "REDEFINE",
      "reason": "BASELINE_DOES_NOT_CONSUME_RECOVERY_PERMISSION",
      "replacement_candidate_id": "capped_recovery_permission_overlay",
      "second_owner_approval_required": true
    },
    {
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "current_route_enabled": false,
      "decision": "WITHDRAW",
      "reason_codes": [
        "NO_CALLABLE_PIT_SOFT_CONFIRMATION",
        "NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST"
      ],
      "reopen_condition": "A future baseline independently introduces a governed callable PIT-valid non-hard defensive request or soft confirmation."
    },
    {
      "candidate_id": "missed_upside_reentry_accelerator",
      "current_route_enabled": false,
      "decision": "REDEFINE",
      "replacement_candidate_id": "post_confirmation_reentry_ramp_accelerator",
      "second_owner_approval_required": true
    }
  ],
  "m2_eligible_candidate_count": 0,
  "m2_eligible_candidate_ids": [],
  "redefine_candidate_count": 2,
  "schema_version": "growth_tilt_candidate_disposition_after_owner_resolution.v1",
  "status": "GROWTH_TILT_OWNER_DECISION_RESOLUTION_READY_WITH_BLOCKERS",
  "withdraw_candidate_count": 1
}
```

## Replacement A Readiness

```json
{
  "approval_status": "BLOCKED_PENDING_M1D2_AND_M1E",
  "blocker_codes": [
    "RECOVERY_PRODUCER_PIT_LINEAGE_VALID",
    "THRESHOLD_VERSIONED_AND_PREREGISTERED",
    "RISK_OFF_VETO_RESOLVED",
    "ALL_ACTUAL_HARD_VETOES_RESOLVED_OR_NOT_APPLICABLE",
    "TRANSITION_TRACE_READY",
    "NATIVE_EXPOSURE_SCALAR_READY",
    "CANDIDATE_CAP_IN_NATIVE_UNITS",
    "SCREENING_POLICY_FROZEN"
  ],
  "conditions": {
    "all_actual_hard_vetoes_resolved_or_not_applicable": false,
    "candidate_cap_in_native_units": false,
    "native_exposure_scalar_ready": false,
    "next_step_timing_registered": true,
    "output_semantic_type_known": true,
    "recovery_producer_pit_lineage_valid": false,
    "risk_off_veto_resolved": false,
    "screening_policy_frozen": false,
    "threshold_versioned_and_preregistered": false,
    "transition_trace_ready": false
  },
  "m2_eligible": false,
  "ready_condition_count": 2,
  "replacement_candidate_id": "capped_recovery_permission_overlay",
  "required_condition_count": 10,
  "schema_version": "growth_tilt_replacement_a_readiness.v1",
  "status": "GROWTH_TILT_OWNER_DECISION_RESOLUTION_READY_WITH_BLOCKERS"
}
```

## M1D2 Scope

```json
{
  "candidate_behavior_allowed": false,
  "implement": [
    "hard_veto_aggregate_adapter",
    "current_requested_applied_regime_transition_trace",
    "baseline_native_exposure_scalar_trace",
    "recovery_producer_pit_lineage_report"
  ],
  "implement_conditionally": [
    "recovery_permission_semantic_adapter_without_baseline_transition"
  ],
  "implementation_allowed": true,
  "prohibited": [
    "new_baseline_recovery_persistence",
    "new_baseline_recovery_transition",
    "new_soft_confirmation",
    "new_aggregate_non_hard_defensive_request",
    "new_event_risk_veto",
    "new_qqq_equivalent_candidate_delta_conversion",
    "real_pit_replay"
  ],
  "replay_allowed": false,
  "schema_version": "growth_tilt_m1d2_adapter_scope.v1",
  "status": "GROWTH_TILT_OWNER_DECISION_RESOLUTION_READY_WITH_BLOCKERS"
}
```

## 结论

owner decisions 已完整，但 recovery PIT lineage、threshold、hard veto、transition trace 和 native scalar 仍有 evidence blockers。M1D2 只获准实现不改变 baseline 决策的 adapters；replacement A 仍不得批准或 replay。
