# Growth Tilt Candidate Research Contract Approval

- task_id: `TRADING-2438M1`
- status: `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_BLOCKED`
- requested date: `2026-07-08`
- market regime: `ai_after_chatgpt`
- next route: `TRADING-2438M1B_GROWTH_TILT_SHARED_METRIC_AND_SCREENING_POLICY_APPROVAL`

M1 只验证 owner-review 输入契约，不运行 replay/backtest/scoring，不修改 candidate parameters 或 threshold values。selection order 来自 config declaration order，不代表业绩排名。M2 只接收 contract 完整的 APPROVE 候选；REDEFINE/WITHDRAW 不阻断其他候选，但自身不得进入 replay。

```json
{
  "approved_candidate_count": 2,
  "as_of": "2026-07-08",
  "candidate_count": 3,
  "data_quality_status": "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACTS_CONFIG_OWNER_REVIEW_ONLY",
  "m2_eligible_candidate_count": 0,
  "m2_eligible_candidate_ids": [],
  "market_regime": "ai_after_chatgpt",
  "metric_contract_ready_count": 0,
  "next_route": "TRADING-2438M1B_GROWTH_TILT_SHARED_METRIC_AND_SCREENING_POLICY_APPROVAL",
  "owner_decision_complete_count": 3,
  "owner_input_gap_count": 6,
  "owner_input_gaps_by_code": {
    "OWNER_RUNTIME_SPEC_INCOMPLETE": 2,
    "OWNER_SCREENING_THRESHOLD_POLICY_INCOMPLETE": 2,
    "OWNER_SHARED_METRIC_CONTRACT_INCOMPLETE": 2
  },
  "pending_candidate_count": 0,
  "performance_ranked": false,
  "redefine_candidate_count": 1,
  "runtime_spec_ready_count": 0,
  "selection_basis": "CONFIG_DECLARATION_ORDER",
  "source_status": "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED",
  "status": "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_BLOCKED",
  "threshold_policy_ready_count": 0,
  "withdraw_candidate_count": 0
}
```

## Candidate Review Summary

```json
[
  {
    "candidate_id": "recovery_reentry_speedup_guard",
    "decision": "APPROVE",
    "gap_codes": [
      "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "OWNER_SCREENING_THRESHOLD_POLICY_INCOMPLETE",
      "OWNER_SHARED_METRIC_CONTRACT_INCOMPLETE"
    ],
    "m2_eligible": false,
    "metric_contract_ready": false,
    "performance_ranked": false,
    "review_status": "APPROVAL_CONTRACT_BLOCKED",
    "runtime_spec_ready": false,
    "selection_basis": "CONFIG_DECLARATION_ORDER",
    "selection_order": 1,
    "threshold_policy_ready": false
  },
  {
    "candidate_id": "false_risk_off_confirmation_relaxation",
    "decision": "APPROVE",
    "gap_codes": [
      "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "OWNER_SCREENING_THRESHOLD_POLICY_INCOMPLETE",
      "OWNER_SHARED_METRIC_CONTRACT_INCOMPLETE"
    ],
    "m2_eligible": false,
    "metric_contract_ready": false,
    "performance_ranked": false,
    "review_status": "APPROVAL_CONTRACT_BLOCKED",
    "runtime_spec_ready": false,
    "selection_basis": "CONFIG_DECLARATION_ORDER",
    "selection_order": 2,
    "threshold_policy_ready": false
  },
  {
    "candidate_id": "missed_upside_reentry_accelerator",
    "decision": "REDEFINE",
    "gap_codes": [],
    "m2_eligible": false,
    "metric_contract_ready": false,
    "performance_ranked": false,
    "review_status": "REDEFINED_SECOND_OWNER_APPROVAL_REQUIRED",
    "runtime_spec_ready": false,
    "selection_basis": "CONFIG_DECLARATION_ORDER",
    "selection_order": 3,
    "threshold_policy_ready": false
  }
]
```

## Owner Action Checklist

```json
{
  "actions": [
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "runtime_spec",
      "gap_code": "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Resolve runtime fields/placeholders: applicable_regime_ids[0], hard_veto_ids[0], parameters.lagging_soft_confirmation_id, parameters.provisional_exposure_absolute_cap, parameters.recovery_signal_id."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "metric_contract_ref",
      "gap_code": "OWNER_SHARED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Approve the shared metric contract: empty_event_policy_unresolved, metric_contract_not_owner_approved, metric_contract_owner_unresolved, metric_definition_incomplete:false_risk_off_delta, metric_definition_incomplete:missed_upside_delta, metric_definition_incomplete:whipsaw_delta, relative_delta_epsilon_owner_unresolved, relative_delta_epsilon_policy_unresolved."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "threshold_policy_ref",
      "gap_code": "OWNER_SCREENING_THRESHOLD_POLICY_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Preregister the candidate screening policy: candidate_thresholds_not_owner_preregistered, threshold_policy_not_owner_approved, threshold_policy_owner_unresolved."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "runtime_spec",
      "gap_code": "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Resolve runtime fields/placeholders: applicable_regime_ids[0], hard_veto_ids[0], parameters.baseline_required_state, parameters.relaxed_soft_confirmation_id."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "metric_contract_ref",
      "gap_code": "OWNER_SHARED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Approve the shared metric contract: empty_event_policy_unresolved, metric_contract_not_owner_approved, metric_contract_owner_unresolved, metric_definition_incomplete:false_risk_off_delta, metric_definition_incomplete:missed_upside_delta, metric_definition_incomplete:whipsaw_delta, relative_delta_epsilon_owner_unresolved, relative_delta_epsilon_policy_unresolved."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "threshold_policy_ref",
      "gap_code": "OWNER_SCREENING_THRESHOLD_POLICY_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Preregister the candidate screening policy: candidate_thresholds_not_owner_preregistered, threshold_policy_not_owner_approved, threshold_policy_owner_unresolved."
    }
  ],
  "open_action_count": 6,
  "schema_version": "growth_tilt_runtime_spec_owner_action_checklist.v2",
  "status": "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_BLOCKED"
}
```

完整 metric/threshold review matrix 与 source provenance 见同目录 JSON。
