# Growth Tilt Replacement Candidate Contract

M1E 只执行 evidence gate。由于 recovery PIT/threshold、hard-veto aggregate、requested/applied transition、native scalar/native cap、screening policy 和 second owner approval 尚未全部闭合，replacement A 保持 REDEFINE/BLOCKED。

```json
{
  "approval_prerequisites_ready": false,
  "approved_candidate_count": 0,
  "as_of": "2026-07-10",
  "disposition": "KEEP_REDEFINED_BLOCKED",
  "m2_eligible_candidate_count": 0,
  "next_route": "TRADING-2438M1E_GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT",
  "replacement_candidate_id": "capped_recovery_permission_overlay",
  "screening_policy_status": "PENDING_OWNER_PREREGISTRATION",
  "status": "GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT_READY_BLOCKED",
  "strict_validation_error_count": 0
}
```

## Prerequisite matrix

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

## Decision

```json
{
  "approval_status": "BLOCKED",
  "approved_executor_binding_emitted": false,
  "approved_runtime_spec_emitted": false,
  "disposition": "KEEP_REDEFINED_BLOCKED",
  "m2_invocation_allowed": false,
  "next_required_action": "OWNER_RECORDS_SECOND_APPROVAL_AFTER_ALL_CONTRACTS_AND_POLICY_ARE_PROVEN",
  "policy_approval_metadata_written": false,
  "post_replay_tuning_allowed": false,
  "replacement_candidate_id": "capped_recovery_permission_overlay",
  "schema_version": "growth_tilt_replacement_candidate_decision.v1"
}
```

## 结论

没有 APPROVE runtime spec、executor binding、policy approval metadata 或 M2 invocation 被创建；approved candidate=0，M2 eligible=0。
