# Growth Tilt Post-Runtime Candidate PIT Replay Blocker Resolution

- task_id: `TRADING-2438M`
- status: `GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED`
- market regime: `ai_after_chatgpt`
- requested date: `2026-07-08`
- data quality: `PASS_WITH_WARNINGS`
- pass / fail / blocked: `0` / `0` / `3`
- next route: `TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval`

本报告只解析 validation-only candidate runtime evidence。当前若缺少受审的 candidate executable spec、真实 replay output 或 threshold policy，结果必须保持 BLOCKED；不得把 null 转为 0，不得从候选名称推断参数，不得把静态 threshold contract 计为 runtime evaluation。

```json
{
  "as_of": "2026-07-08",
  "candidate_count": 3,
  "completed_threshold_evaluation_count": 0,
  "computed_runtime_metric_count": 0,
  "data_quality_status": "PASS_WITH_WARNINGS",
  "market_regime": "ai_after_chatgpt",
  "missing_threshold_evaluation_count": 3,
  "next_route": "TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval",
  "null_runtime_metric_count": 18,
  "pass_fail_blocked": [
    0,
    0,
    3
  ],
  "runtime_invoked_candidate_count": 0,
  "status": "GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED",
  "unresolved_blocker_count": 33
}
```

## Candidate Stage Summary

```json
[
  {
    "blocker_codes": [
      "CANDIDATE_RUNTIME_INPUT_CONTRACT_MISSING",
      "CANDIDATE_RUNTIME_METRIC_DEPENDENCY_UNRESOLVED",
      "CANDIDATE_RUNTIME_REPLAY_RUNNER_NOT_INVOKED",
      "CANDIDATE_RUNTIME_THRESHOLD_EVALUATOR_NOT_INVOKED",
      "CANDIDATE_RUNTIME_THRESHOLD_SPEC_MISSING"
    ],
    "candidate_id": "recovery_reentry_speedup_guard",
    "first_failed_stage": "RUNTIME_INPUT_HYDRATED",
    "outcome_status": "BLOCKED",
    "runtime_executable": true,
    "source_rank": 1
  },
  {
    "blocker_codes": [
      "CANDIDATE_RUNTIME_INPUT_CONTRACT_MISSING",
      "CANDIDATE_RUNTIME_METRIC_DEPENDENCY_UNRESOLVED",
      "CANDIDATE_RUNTIME_REPLAY_RUNNER_NOT_INVOKED",
      "CANDIDATE_RUNTIME_THRESHOLD_EVALUATOR_NOT_INVOKED",
      "CANDIDATE_RUNTIME_THRESHOLD_SPEC_MISSING"
    ],
    "candidate_id": "false_risk_off_confirmation_relaxation",
    "first_failed_stage": "RUNTIME_INPUT_HYDRATED",
    "outcome_status": "BLOCKED",
    "runtime_executable": true,
    "source_rank": 2
  },
  {
    "blocker_codes": [
      "CANDIDATE_RUNTIME_INPUT_CONTRACT_MISSING",
      "CANDIDATE_RUNTIME_METRIC_DEPENDENCY_UNRESOLVED",
      "CANDIDATE_RUNTIME_REPLAY_RUNNER_NOT_INVOKED",
      "CANDIDATE_RUNTIME_THRESHOLD_EVALUATOR_NOT_INVOKED",
      "CANDIDATE_RUNTIME_THRESHOLD_SPEC_MISSING"
    ],
    "candidate_id": "missed_upside_reentry_accelerator",
    "first_failed_stage": "RUNTIME_INPUT_HYDRATED",
    "outcome_status": "BLOCKED",
    "runtime_executable": true,
    "source_rank": 3
  }
]
```

## Blocker Taxonomy Counts

```json
{
  "CANDIDATE_RUNTIME_INPUT_CONTRACT_MISSING": 3,
  "CANDIDATE_RUNTIME_METRIC_DEPENDENCY_UNRESOLVED": 21,
  "CANDIDATE_RUNTIME_REPLAY_RUNNER_NOT_INVOKED": 3,
  "CANDIDATE_RUNTIME_THRESHOLD_EVALUATOR_NOT_INVOKED": 3,
  "CANDIDATE_RUNTIME_THRESHOLD_SPEC_MISSING": 3
}
```

完整 stage trace、metric materialization、threshold evaluations、blocker records 和 provenance 见同目录 supporting JSON artifacts。
