# Dynamic strategy recombination line plateau 决策

## 结论摘要

- status：`DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_AND_DATA_SIGNAL_QUALITY_DECISION_READY`
- as_of：`2026-07-07`
- owner decision：`PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`
- base candidate：`growth_tilt_lower_turnover_guarded_transfer_v1`
- best targeted variant：`growth_tilt_guarded_transfer_valid_until_strict_v1`
- recombination line plateau detected：`True`
- continue local targeted improvement recommended：`False`
- next route：`TRADING-2402_Dynamic_Strategy_Data_PIT_And_Signal_Quality_Gap_Review`

## TRADING-2396 至 TRADING-2400 的来源结论

{
  "trading_2396": {
    "best_recombination_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "best_recombination_decision": "OWNER_REVIEW_REQUIRED",
    "status": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY"
  },
  "trading_2397": {
    "owner_decision": "KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE",
    "status": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
  },
  "trading_2398": {
    "planned_targeted_variants": 6,
    "status": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY"
  },
  "trading_2399": {
    "best_targeted_variant": "growth_tilt_guarded_transfer_valid_until_strict_v1",
    "best_targeted_variant_decision": "CONTINUE_TARGETED_IMPROVEMENT",
    "observation_preview_candidates_count": 0,
    "status": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_READY"
  },
  "trading_2400": {
    "data_signal_quality_review_recommended": true,
    "owner_decision": "DO_NOT_APPROVE_OBSERVATION_RETAIN_TARGETED_IMPROVEMENT_VALUE_AND_REQUIRE_PLATEAU_REVIEW",
    "plateau_review_required": true,
    "status": "DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY",
    "threshold_meta_dataset_recommended": true
  }
}

## Recombination line 历史

{
  "TRADING-2396": {
    "best_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "decision": "OWNER_REVIEW_REQUIRED",
    "result": "recombination candidate retest 已完成"
  },
  "TRADING-2397": {
    "owner_decision": "KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE",
    "result": "owner review decision 已记录"
  },
  "TRADING-2398": {
    "planned_targeted_variants": 6,
    "result": "gate evidence 与 targeted improvement plan 已形成"
  },
  "TRADING-2399": {
    "best_targeted_variant": "growth_tilt_guarded_transfer_valid_until_strict_v1",
    "decision": "CONTINUE_TARGETED_IMPROVEMENT",
    "observation_preview_candidates_count": 0,
    "result": "targeted gate evidence retest 已完成"
  },
  "TRADING-2400": {
    "owner_decision": "DO_NOT_APPROVE_OBSERVATION_RETAIN_TARGETED_IMPROVEMENT_VALUE_AND_REQUIRE_PLATEAU_REVIEW",
    "result": "owner non-approval decision 已记录"
  }
}

## Plateau 评估

{
  "plateau_criteria": {
    "best_variant_still_below_observation_preview": {
      "expected": true,
      "observed": true,
      "passed": true
    },
    "data_signal_quality_review_recommended": {
      "expected": true,
      "observed": true,
      "passed": true
    },
    "more_local_variant_search_has_diminishing_return_risk": {
      "expected": true,
      "observed": true,
      "passed": true
    },
    "no_observation_preview_candidate_after_targeted_retest": {
      "expected": true,
      "observed": true,
      "passed": true
    },
    "targeted_improvement_value_retained": {
      "expected": true,
      "observed": true,
      "passed": true
    },
    "threshold_meta_dataset_recommended": {
      "expected": true,
      "observed": true,
      "passed": true
    }
  },
  "plateau_evidence": [
    "2396 只产生 OWNER_REVIEW_REQUIRED，未产生 observation preview",
    "2397 保留 owner review，未批准 observation",
    "2398 规划了 6 个 targeted variants",
    "2399 最佳 targeted variant 仍为 CONTINUE_TARGETED_IMPROVEMENT",
    "2399 observation preview candidate count 仍为 0",
    "2400 明确要求进入 plateau / data-signal quality review"
  ],
  "plateau_scope": "growth_tilt_lower_turnover_guarded_transfer_line",
  "primary_blockers": [
    "targeted retest 后仍没有 observation preview candidate",
    "继续做局部 variant 搜索存在边际收益递减风险",
    "data / PIT / signal quality 和 regime labeling 可能限制证据质量",
    "threshold calibration 仍缺少 meta-dataset"
  ],
  "recombination_line_history": {
    "TRADING-2396": {
      "best_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "decision": "OWNER_REVIEW_REQUIRED",
      "result": "recombination candidate retest 已完成"
    },
    "TRADING-2397": {
      "owner_decision": "KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE",
      "result": "owner review decision 已记录"
    },
    "TRADING-2398": {
      "planned_targeted_variants": 6,
      "result": "gate evidence 与 targeted improvement plan 已形成"
    },
    "TRADING-2399": {
      "best_targeted_variant": "growth_tilt_guarded_transfer_valid_until_strict_v1",
      "decision": "CONTINUE_TARGETED_IMPROVEMENT",
      "observation_preview_candidates_count": 0,
      "result": "targeted gate evidence retest 已完成"
    },
    "TRADING-2400": {
      "owner_decision": "DO_NOT_APPROVE_OBSERVATION_RETAIN_TARGETED_IMPROVEMENT_VALUE_AND_REQUIRE_PLATEAU_REVIEW",
      "result": "owner non-approval decision 已记录"
    }
  },
  "recombination_line_plateau_detected": true,
  "record_ready": true,
  "schema_version": "dynamic_strategy_recombination_plateau_review.v1"
}

## 为什么仍不批准 observation

- targeted retest 后仍没有 observation preview candidate。
- best targeted variant 仍停留在 CONTINUE_TARGETED_IMPROVEMENT。
- 2400 owner decision 明确保留 non-approval。
- 下一步是 evidence-quality review，不是 execution exposure。

## Targeted improvement value 保留

- valid-until strict direction 仍是有研究价值的 reference。
- 保留研究价值不等于 observation readiness。

## 下一步方向选项

{
  "continue_local_targeted_improvement_recommended": false,
  "data_signal_quality_review_recommended": true,
  "decision_options": {
    "OPTION_A_CONTINUE_LOCAL_TARGETED_IMPROVEMENT": {
      "meaning": "继续生成小幅 valid-until strict / guarded transfer variants",
      "recommended": false,
      "risk": "边际收益递减风险高"
    },
    "OPTION_B_PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY": {
      "meaning": "暂停当前 recombination line，复盘 data quality、PIT coverage、signal quality 和 regime labeling",
      "recommended": true
    },
    "OPTION_C_BUILD_THRESHOLD_META_DATASET_FIRST": {
      "meaning": "先构建 threshold calibration meta-dataset，再判断 candidate gates 是否过严",
      "recommended": true
    },
    "OPTION_D_REVISIT_SIGNAL_CONSTRUCTION_FRAMEWORK": {
      "meaning": "回到 indicator -> signal -> weight mapping，评估底层 signal quality",
      "recommended": false
    },
    "OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW": {
      "meaning": "暂时停止 dynamic strategy research line",
      "recommended": false
    }
  },
  "owner_decision": "PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY",
  "pit_coverage_review_recommended": true,
  "recommended_default_option": "OPTION_B_PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY",
  "record_ready": true,
  "regime_labeling_review_recommended": true,
  "secondary_recommended_option": "OPTION_C_BUILD_THRESHOLD_META_DATASET_FIRST",
  "threshold_meta_dataset_recommended": true
}

## 推荐下一步方向

- `OPTION_B_PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`
- secondary：`OPTION_C_BUILD_THRESHOLD_META_DATASET_FIRST`

## Data / PIT / signal quality 复盘范围

{
  "PIT_coverage": [
    "评估当前 PIT approximation 是否足以支持 signal validation",
    "识别缺少 point-in-time history 的 features 或 signals",
    "判断是否需要更多 history artifacts 或外部数据源"
  ],
  "data_quality": [
    "评估 PASS_WITH_WARNINGS 警告是否影响 dynamic strategy research",
    "复盘 cached market data 覆盖范围与 stale data 风险",
    "复盘 survivorship、lookahead 与 corporate-action 处理",
    "复盘 missing values 与 source reconciliation"
  ],
  "regime_labeling": [
    "复盘 risk_on / risk_off / high_vol / low_vol / recovery labels",
    "检验 regime expectation 是否过粗",
    "评估用 regime_expectation_score 替代 regime_slice_pass_rate"
  ],
  "signal_quality": [
    "复盘 growth_tilt_engine signal source stability",
    "复盘 valid_until_window signal expiry assumptions",
    "复盘 signal-to-execution lag accuracy",
    "复盘 stale signal detection coverage"
  ],
  "threshold_meta_dataset": [
    "校准 time_slice_pass_rate threshold",
    "校准 regime_expectation_score threshold",
    "校准 drawdown materiality threshold"
  ]
}

## 明确不批准事项

- `candidate_auto_accept`
- `research_only_observation`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
- `event_append`
- `outcome_binding`
- `scheduler`
- `scheduled_task`
- `daily_report`
- `production`
- `broker_order`
- `new_backtest`
- `new_signal`
- `new_scoring`

## 推荐下一任务

- `TRADING-2402_Dynamic_Strategy_Data_PIT_And_Signal_Quality_Gap_Review`
