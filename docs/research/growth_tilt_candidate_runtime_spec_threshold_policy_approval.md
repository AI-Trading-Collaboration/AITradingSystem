# Growth Tilt Candidate Runtime Spec And Threshold Policy Approval

- task_id: `TRADING-2438M1`
- status: `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_BLOCKED_OWNER_INPUT`
- requested date: `2026-07-08`
- market regime: `ai_after_chatgpt`
- next route: `TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval`

M1 只验证 owner-review 输入契约，不运行 replay/backtest/scoring，不修改 candidate parameters 或 threshold values。PENDING、REDEFINE、WITHDRAW 和 incomplete APPROVE 均保持 fail-closed。

```json
{
  "approved_candidate_count": 0,
  "as_of": "2026-07-08",
  "candidate_count": 3,
  "data_quality_status": "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACTS_CONFIG_OWNER_REVIEW_ONLY",
  "market_regime": "ai_after_chatgpt",
  "metric_contract_ready_count": 0,
  "next_route": "TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval",
  "owner_input_gap_count": 27,
  "owner_input_gaps_by_code": {
    "OWNER_CANDIDATE_DECISION_PENDING": 3,
    "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE": 18,
    "OWNER_RUNTIME_SPEC_INCOMPLETE": 3,
    "OWNER_THRESHOLD_POLICY_INCOMPLETE": 3
  },
  "pending_candidate_count": 3,
  "redefine_candidate_count": 0,
  "runtime_spec_ready_count": 0,
  "source_status": "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED",
  "status": "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_BLOCKED_OWNER_INPUT",
  "threshold_policy_ready_count": 0,
  "withdraw_candidate_count": 0
}
```

## Candidate Review Summary

```json
[
  {
    "candidate_id": "recovery_reentry_speedup_guard",
    "decision": "PENDING",
    "gap_codes": [
      "OWNER_CANDIDATE_DECISION_PENDING",
      "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "OWNER_THRESHOLD_POLICY_INCOMPLETE"
    ],
    "metric_contract_ready": false,
    "review_status": "PENDING",
    "runtime_spec_ready": false,
    "source_rank": 1,
    "threshold_policy_ready": false
  },
  {
    "candidate_id": "false_risk_off_confirmation_relaxation",
    "decision": "PENDING",
    "gap_codes": [
      "OWNER_CANDIDATE_DECISION_PENDING",
      "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "OWNER_THRESHOLD_POLICY_INCOMPLETE"
    ],
    "metric_contract_ready": false,
    "review_status": "PENDING",
    "runtime_spec_ready": false,
    "source_rank": 2,
    "threshold_policy_ready": false
  },
  {
    "candidate_id": "missed_upside_reentry_accelerator",
    "decision": "PENDING",
    "gap_codes": [
      "OWNER_CANDIDATE_DECISION_PENDING",
      "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "OWNER_THRESHOLD_POLICY_INCOMPLETE"
    ],
    "metric_contract_ready": false,
    "review_status": "PENDING",
    "runtime_spec_ready": false,
    "source_rank": 3,
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
      "field_path": "decision",
      "gap_code": "OWNER_CANDIDATE_DECISION_PENDING",
      "production_effect": "none",
      "recommended_action": "Record an explicit APPROVE, REDEFINE, or WITHDRAW owner decision."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "runtime_spec",
      "gap_code": "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Provide the executable candidate parameter contract if approving."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "metric_specs.return_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "metric_specs.max_drawdown_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "metric_specs.turnover_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "metric_specs.false_risk_off_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "metric_specs.missed_upside_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "metric_specs.whipsaw_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "field_path": "threshold_specs.missing_threshold_spec",
      "gap_code": "OWNER_THRESHOLD_POLICY_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete the governed threshold record without deriving a value from current metrics or prior runs."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "decision",
      "gap_code": "OWNER_CANDIDATE_DECISION_PENDING",
      "production_effect": "none",
      "recommended_action": "Record an explicit APPROVE, REDEFINE, or WITHDRAW owner decision."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "runtime_spec",
      "gap_code": "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Provide the executable candidate parameter contract if approving."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "metric_specs.return_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "metric_specs.max_drawdown_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "metric_specs.turnover_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "metric_specs.false_risk_off_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "metric_specs.missed_upside_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "metric_specs.whipsaw_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "field_path": "threshold_specs.missing_threshold_spec",
      "gap_code": "OWNER_THRESHOLD_POLICY_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete the governed threshold record without deriving a value from current metrics or prior runs."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "decision",
      "gap_code": "OWNER_CANDIDATE_DECISION_PENDING",
      "production_effect": "none",
      "recommended_action": "Record an explicit APPROVE, REDEFINE, or WITHDRAW owner decision."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "runtime_spec",
      "gap_code": "OWNER_RUNTIME_SPEC_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Provide the executable candidate parameter contract if approving."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "metric_specs.return_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "metric_specs.max_drawdown_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "metric_specs.turnover_delta_vs_baseline",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "metric_specs.false_risk_off_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "metric_specs.missed_upside_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "metric_specs.whipsaw_delta",
      "gap_code": "OWNER_REQUIRED_METRIC_CONTRACT_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete metric contract fields: source_field, unit, normalization_rule_id, calculator_id, calculator_version."
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "field_path": "threshold_specs.missing_threshold_spec",
      "gap_code": "OWNER_THRESHOLD_POLICY_INCOMPLETE",
      "production_effect": "none",
      "recommended_action": "Complete the governed threshold record without deriving a value from current metrics or prior runs."
    }
  ],
  "open_action_count": 27,
  "schema_version": "growth_tilt_runtime_spec_owner_action_checklist.v1",
  "status": "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_BLOCKED_OWNER_INPUT"
}
```

完整 metric/threshold review matrix 与 source provenance 见同目录 JSON。
