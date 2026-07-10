# Growth Tilt Candidate Research Contract Approval

- task_id: `TRADING-2438M1`
- status: `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVED`
- requested date: `2026-07-08`
- market regime: `ai_after_chatgpt`
- next route: `TRADING-2438N_GROWTH_TILT_NO_APPROVED_CANDIDATE_DISPOSITION`

M1 只验证 owner-review 输入契约，不运行 replay/backtest/scoring，不修改 candidate parameters 或 threshold values。selection order 来自 config declaration order，不代表业绩排名。M2 只接收 contract 完整的 APPROVE 候选；REDEFINE/WITHDRAW 不阻断其他候选，但自身不得进入 replay。

```json
{
  "approved_candidate_count": 0,
  "as_of": "2026-07-08",
  "candidate_count": 3,
  "data_quality_status": "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACTS_CONFIG_OWNER_REVIEW_ONLY",
  "m2_eligible_candidate_count": 0,
  "m2_eligible_candidate_ids": [],
  "market_regime": "ai_after_chatgpt",
  "metric_contract_ready_count": 0,
  "next_route": "TRADING-2438N_GROWTH_TILT_NO_APPROVED_CANDIDATE_DISPOSITION",
  "owner_decision_complete_count": 3,
  "owner_input_gap_count": 0,
  "owner_input_gaps_by_code": {},
  "pending_candidate_count": 0,
  "performance_ranked": false,
  "redefine_candidate_count": 2,
  "runtime_spec_ready_count": 0,
  "selection_basis": "CONFIG_DECLARATION_ORDER",
  "source_status": "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED",
  "status": "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVED",
  "threshold_policy_ready_count": 0,
  "withdraw_candidate_count": 1
}
```

## Candidate Review Summary

```json
[
  {
    "candidate_id": "recovery_reentry_speedup_guard",
    "decision": "REDEFINE",
    "gap_codes": [],
    "m2_eligible": false,
    "metric_contract_ready": false,
    "performance_ranked": false,
    "review_status": "REDEFINED_SECOND_OWNER_APPROVAL_REQUIRED",
    "runtime_spec_ready": false,
    "selection_basis": "CONFIG_DECLARATION_ORDER",
    "selection_order": 1,
    "threshold_policy_ready": false
  },
  {
    "candidate_id": "false_risk_off_confirmation_relaxation",
    "decision": "WITHDRAW",
    "gap_codes": [],
    "m2_eligible": false,
    "metric_contract_ready": false,
    "performance_ranked": false,
    "review_status": "WITHDRAWN",
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
  "actions": [],
  "open_action_count": 0,
  "schema_version": "growth_tilt_runtime_spec_owner_action_checklist.v2",
  "status": "GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVED"
}
```

完整 metric/threshold review matrix 与 source provenance 见同目录 JSON。
