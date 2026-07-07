# Dynamic strategy data signal quality 下一步方向

- status：`DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_AND_DATA_SIGNAL_QUALITY_DECISION_READY`
- owner decision：`PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`
- recommended default option：`OPTION_B_PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`
- secondary option：`OPTION_C_BUILD_THRESHOLD_META_DATASET_FIRST`

## 复盘范围

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

## 决策选项

{
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
}
