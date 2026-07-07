# Dynamic strategy data / PIT / signal quality gap review

## 结论摘要

- status：`DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`
- validate-data：`PASS_WITH_WARNINGS`；errors=`0`；warnings=`2`
- recombination line paused：`True`
- resume candidate search recommended：`False`
- next route：`TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review`

## Source findings from TRADING-2401

- owner decision：`PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`
- 2401 已确认 plateau，2402 只做 gap review，不恢复候选搜索。

## Data quality review

{
  "cached_market_data": {
    "corporate_action_handling": "known split events are recorded; one TQQQ adjustment-ratio warning remains reviewable",
    "coverage_end": "2026-07-02",
    "coverage_start": "2018-01-02",
    "macro_coverage_end": "2026-07-01",
    "macro_coverage_start": "2018-01-02",
    "macro_rate_row_count": 6365,
    "missing_date_count": "NOT_EXPOSED_BY_VALIDATE_DATA_AUDIT",
    "missing_symbol_count": "NOT_EXPOSED_BY_VALIDATE_DATA_AUDIT",
    "price_row_count": 56288,
    "secondary_coverage_end": "2026-07-02",
    "secondary_coverage_start": "2018-01-02",
    "secondary_price_row_count": 51769,
    "split_dividend_adjustment_risk": "MATERIAL_TQQQ_ADJUSTMENT_RATIO_WARNING",
    "stale_data_risk": "MINOR_WEEKEND_OR_HOLIDAY_AS_OF_WITH_LAST_PRICE_2026-07-02"
  },
  "error_count": 0,
  "info_count": 12,
  "latest_validate_data_status": "PASS_WITH_WARNINGS",
  "pass_with_warnings_interpretation": "PASS_WITH_WARNINGS 不阻断 2402 review；但 TQQQ adjustment-ratio warning 直接触及 dynamic strategy universe，后续候选解释必须保留 caveat。",
  "record_ready": true,
  "schema_version": "dynamic_strategy_data_quality_gap_review.v1",
  "warning_count": 2,
  "warning_detail_summary": [
    {
      "code": "prices_download_manifest_checksum_missing",
      "description": "价格数据当前文件 sha256 未出现在下载审计清单中；请确认缓存是否由 download-data 生成。",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "cache provenance is incomplete; ranking math not directly changed but auditability is weakened",
      "recommended_fix": "reconcile price cache checksum with download_manifest or rerun audited download-data path",
      "row_count": "",
      "sample": "D:\\Work\\AITradingSystem\\data\\raw\\prices_daily.csv",
      "severity": "警告",
      "source": "下载审计清单"
    },
    {
      "code": "prices_adjustment_ratio_jump",
      "description": "价格数据的复权比例出现明显跳变",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "TQQQ is part of the dynamic strategy universe; unresolved adjustment-ratio warning can affect leveraged exposure interpretation",
      "recommended_fix": "investigate TQQQ corporate-action / adjusted-close ratio and document whether it is vendor basis or cache error",
      "row_count": "1",
      "sample": "{'date': '2025-11-20', 'ticker': 'TQQQ', '_adjustment_ratio': 0.9946178686759957, '_adjustment_ratio_change': 1.0000294200104527}",
      "severity": "警告",
      "source": "价格主源"
    }
  ],
  "warnings_irrelevant_to_dynamic_strategy": [],
  "warnings_relevant_to_dynamic_strategy": [
    {
      "code": "prices_download_manifest_checksum_missing",
      "description": "价格数据当前文件 sha256 未出现在下载审计清单中；请确认缓存是否由 download-data 生成。",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "cache provenance is incomplete; ranking math not directly changed but auditability is weakened",
      "recommended_fix": "reconcile price cache checksum with download_manifest or rerun audited download-data path",
      "row_count": "",
      "sample": "D:\\Work\\AITradingSystem\\data\\raw\\prices_daily.csv",
      "severity": "警告",
      "source": "下载审计清单"
    },
    {
      "code": "prices_adjustment_ratio_jump",
      "description": "价格数据的复权比例出现明显跳变",
      "dynamic_strategy_relevance": "MATERIAL",
      "gap_category": "DATA_QUALITY",
      "likely_impact": "TQQQ is part of the dynamic strategy universe; unresolved adjustment-ratio warning can affect leveraged exposure interpretation",
      "recommended_fix": "investigate TQQQ corporate-action / adjusted-close ratio and document whether it is vendor basis or cache error",
      "row_count": "1",
      "sample": "{'date': '2025-11-20', 'ticker': 'TQQQ', '_adjustment_ratio': 0.9946178686759957, '_adjustment_ratio_change': 1.0000294200104527}",
      "severity": "警告",
      "source": "价格主源"
    }
  ]
}

## PIT coverage review

{
  "feature_pit_status": {
    "which_features_are_approximate_pit": [
      "dynamic strategy regime labels derived from historical price behavior",
      "signal valid-until window derived from research policy rather than observed signal expiry distribution"
    ],
    "which_features_are_not_pit_safe": [],
    "which_features_are_point_in_time": [
      "cached QQQ/TQQQ/SGOV prices are historical rows validated by as_of gate",
      "rates_daily.csv has source checksums and date range in validate-data audit"
    ],
    "which_features_depend_on_later_data": []
  },
  "outcome_binding_status": {
    "future_outcome_dependency_risk": "NO_MUTATION_IN_2402_BUT_PIT_MATRIX_REQUIRED_BEFORE_OBSERVATION",
    "no_mutation_confirmed": true,
    "outcome_binding_disabled": true
  },
  "pit_gap_severity": {
    "feature_signal_pit_matrix_missing": "MATERIAL",
    "outcome_binding": "NOT_APPLICABLE",
    "valid_until_pit_lineage_missing": "MATERIAL"
  },
  "record_ready": true,
  "schema_version": "dynamic_strategy_pit_coverage_gap_review.v1",
  "signal_pit_status": {
    "advisory_valid_from_correctness": "MATERIAL_REVIEW_REQUIRED",
    "advisory_valid_until_correctness": "MATERIAL_REVIEW_REQUIRED",
    "revision_or_restated_data_risk": "LOW_FOR_PRICE_ROWS_BUT_NOT_FULLY_AUDITED_FOR_SIGNAL_DERIVED_FEATURES",
    "signal_generation_as_of_date_correctness": "MATERIAL_REVIEW_REQUIRED",
    "signal_horizon_definition": "MATERIAL_REVIEW_REQUIRED"
  },
  "source_scope_from_2401": [
    "评估当前 PIT approximation 是否足以支持 signal validation",
    "识别缺少 point-in-time history 的 features 或 signals",
    "判断是否需要更多 history artifacts 或外部数据源"
  ]
}

## Signal quality review

{
  "best_signal_family_from_2386": {
    "family_average_score": 0.523172,
    "family_best_candidate": "dynamic_valid_until_expiry_strict_v1",
    "family_best_candidate_decision": "CONTINUE_OPTIMIZATION",
    "family_candidate_count": 2,
    "family_failure_reason": "regime_slice_stability_failure",
    "family_rank": 1,
    "family_regime_slice_pass_rate": 0.0,
    "family_time_slice_pass_rate": 0.214285,
    "owner_review_candidate_count": 0,
    "signal_family": "signal_age_valid_until_family"
  },
  "best_targeted_variant": "growth_tilt_guarded_transfer_valid_until_strict_v1",
  "best_targeted_variant_decision": "CONTINUE_TARGETED_IMPROVEMENT",
  "candidate_plateau_interpretation": "更可能是 signal / PIT / regime / threshold evidence 质量限制，而不是单纯缺少局部 recombination variants。",
  "growth_tilt_engine": {
    "false_negative_risk": "MATERIAL_MISSED_SIGNAL_COUNT_NONZERO",
    "false_positive_risk": "MATERIAL_REGIME_AND_DRAWDOWN_FAILURE_RISK",
    "historical_stability": "MATERIAL_REVIEW_REQUIRED",
    "signal_confidence_if_available": "NOT_EXPOSED",
    "signal_decay_rule": "APPROXIMATE_VALID_UNTIL_STRICTNESS",
    "signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
    "source_features": [
      "growth_tilt_engine",
      "guarded_turnover_transfer"
    ],
    "valid_until_rule": "validity_10d_v1 / valid_until_window family"
  },
  "lower_turnover_guardrail": {
    "cooldown_rule_source": "min_holding and cooldown policy from prior retests",
    "effect_on_cost_adjusted_return": "cost stress often survives but return gap remains",
    "effect_on_return_gap": "guardrail can preserve cost but may cap growth tilt upside",
    "max_step_delta_source": "targeted variant pilot constants",
    "turnover_budget_source": "research policy / targeted variant construction"
  },
  "observation_preview_candidates_count": 0,
  "record_ready": true,
  "schema_version": "dynamic_strategy_signal_quality_gap_review.v1",
  "valid_until_strictness": {
    "near_expiry_signal_behavior": "NOT_SEPARATELY_VALIDATED",
    "signal_to_execution_lag_days": 1.0,
    "stale_signal_execution_count": 0,
    "strict_expiry_tradeoff": "strict expiry removes stale carry but may increase missed signals or turnover tradeoff"
  }
}

## Valid-until / stale signal review

{
  "near_expiry_signal_behavior": "NOT_SEPARATELY_VALIDATED",
  "signal_to_execution_lag_days": 1.0,
  "stale_signal_execution_count": 0,
  "strict_expiry_tradeoff": "strict expiry removes stale carry but may increase missed signals or turnover tradeoff"
}

## Regime labeling review

{
  "current_regime_labels": [
    "risk_on",
    "risk_off",
    "high_volatility",
    "low_volatility",
    "trend_confirmed",
    "recovery"
  ],
  "reason": "不同策略在不同 regime 的预期行为不同，不能用统一 pass/fail 标准评估所有regime；应转为 strategy-specific expectation score。",
  "record_ready": true,
  "regime_expectation_not_weak": false,
  "regime_expectation_policy_needed": true,
  "regime_expectation_score_from_best_variant": 0.362364,
  "review_questions": {
    "aligned_with_strategy_expected_behavior": "NOT_YET_CALIBRATED",
    "labels_from_explicit_rules": "PARTIAL_PRIOR_ARTIFACT_RULES_EXIST_BUT_NOT_NORMALIZED",
    "labels_too_coarse": true,
    "lookahead_risk": "MATERIAL_REVIEW_REQUIRED",
    "replace_regime_slice_pass_rate": "RECOMMEND_REGIME_EXPECTATION_SCORE",
    "should_growth_tilt_outperform_static_in_all_regimes": false
  },
  "schema_version": "dynamic_strategy_regime_labeling_gap_review.v1"
}

## Threshold meta-dataset review

{
  "build_before_more_candidate_search": true,
  "current_status": {
    "candidate_results_exist_across_many_tasks": true,
    "current_2399_candidate_decisions": {
      "growth_tilt_guarded_transfer_balanced_gate_v1": "CONTINUE_TARGETED_IMPROVEMENT",
      "growth_tilt_guarded_transfer_drawdown_calibrated_v1": "CONTINUE_TARGETED_IMPROVEMENT",
      "growth_tilt_guarded_transfer_regime_repair_v1": "CONTINUE_TARGETED_IMPROVEMENT",
      "growth_tilt_guarded_transfer_return_retention_v1": "CONTINUE_TARGETED_IMPROVEMENT",
      "growth_tilt_guarded_transfer_time_slice_repair_v1": "CONTINUE_TARGETED_IMPROVEMENT",
      "growth_tilt_guarded_transfer_valid_until_strict_v1": "CONTINUE_TARGETED_IMPROVEMENT"
    },
    "no_full_meta_dataset_yet": true,
    "results_need_normalization_into_matrix": true
  },
  "needed_for": [
    "time_slice_pass_rate_threshold",
    "regime_expectation_score_threshold",
    "drawdown_materiality_threshold",
    "return_per_drawdown_penalty_threshold",
    "owner_review_required_vs_continue_optimization_boundary"
  ],
  "proposed_matrix_dimensions": [
    "candidate_id",
    "source_task",
    "execution_cadence",
    "cost_stress",
    "time_slice_pass_rate",
    "regime_slice_or_expectation_score",
    "dynamic_vs_static_gap",
    "drawdown_gap",
    "turnover",
    "decision",
    "subsequent_reclassification"
  ],
  "record_ready": true,
  "schema_version": "dynamic_strategy_threshold_meta_dataset_gap_review.v1"
}

## Prioritized gap matrix

[
  {
    "affected_candidates": [
      "QQQ",
      "TQQQ",
      "SGOV"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399"
    ],
    "gap_category": "DATA_QUALITY",
    "gap_description": "价格数据当前文件 sha256 未出现在下载审计清单中；请确认缓存是否由 download-data 生成。",
    "gap_id": "2402-DATA-01",
    "likely_impact": "cache provenance is incomplete; ranking math not directly changed but auditability is weakened",
    "owner_review_required": true,
    "recommended_fix": "reconcile price cache checksum with download_manifest or rerun audited download-data path",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "QQQ",
      "TQQQ",
      "SGOV"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399"
    ],
    "gap_category": "DATA_QUALITY",
    "gap_description": "价格数据的复权比例出现明显跳变",
    "gap_id": "2402-DATA-02",
    "likely_impact": "TQQQ is part of the dynamic strategy universe; unresolved adjustment-ratio warning can affect leveraged exposure interpretation",
    "owner_review_required": true,
    "recommended_fix": "investigate TQQQ corporate-action / adjusted-close ratio and document whether it is vendor basis or cache error",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "dynamic strategy candidate family"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399",
      "TRADING-2400",
      "TRADING-2401"
    ],
    "gap_category": "PIT_COVERAGE",
    "gap_description": "feature / signal / advisory valid-from / valid-until PIT coverage matrix is missing",
    "gap_id": "2402-PIT-01",
    "likely_impact": "candidate ranking cannot be promoted to observation without explicit PIT lineage",
    "owner_review_required": true,
    "recommended_fix": "build PIT coverage matrix for feature, signal, advisory and outcome fields",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "growth_tilt_lower_turnover_guarded_transfer_v1",
      "growth_tilt_guarded_transfer_valid_until_strict_v1"
    ],
    "affected_research_tasks": [
      "TRADING-2386",
      "TRADING-2399",
      "TRADING-2401"
    ],
    "gap_category": "SIGNAL_QUALITY",
    "gap_description": "growth_tilt / valid_until signal quality is not separately validated from portfolio-combination effects",
    "gap_id": "2402-SIGNAL-01",
    "likely_impact": "更可能是 signal / PIT / regime / threshold evidence 质量限制，而不是单纯缺少局部 recombination variants。",
    "owner_review_required": true,
    "recommended_fix": "review signal construction framework and isolate signal stability / false-positive / false-negative behavior",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "growth_tilt_guarded_transfer_valid_until_strict_v1"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399"
    ],
    "gap_category": "VALID_UNTIL_AND_STALE_SIGNAL",
    "gap_description": "valid-until window is still a research policy approximation rather than calibrated signal expiry evidence",
    "gap_id": "2402-VALIDUNTIL-01",
    "likely_impact": "stale-signal and missed-signal tradeoff can change candidate ranking",
    "owner_review_required": true,
    "recommended_fix": "build signal-age / expiry evidence before further valid-until variants",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "growth_tilt strategy family"
    ],
    "affected_research_tasks": [
      "TRADING-2386",
      "TRADING-2399"
    ],
    "gap_category": "REGIME_LABELING",
    "gap_description": "regime labels are too coarse for strategy-specific expected behavior",
    "gap_id": "2402-REGIME-01",
    "likely_impact": "不同策略在不同 regime 的预期行为不同，不能用统一 pass/fail 标准评估所有regime；应转为 strategy-specific expectation score。",
    "owner_review_required": true,
    "recommended_fix": "replace raw regime pass-rate with regime expectation score policy",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "all dynamic strategy candidates"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399",
      "TRADING-2400",
      "TRADING-2401"
    ],
    "gap_category": "THRESHOLD_CALIBRATION",
    "gap_description": "candidate gate thresholds lack normalized historical meta-dataset",
    "gap_id": "2402-THRESHOLD-01",
    "likely_impact": "owner-review vs continue-optimization boundary remains subjective",
    "owner_review_required": true,
    "recommended_fix": "normalize candidate x threshold x decision history into a calibration matrix",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MATERIAL"
  },
  {
    "affected_candidates": [
      "all dynamic strategy candidates"
    ],
    "affected_research_tasks": [
      "TRADING-2364",
      "TRADING-2386",
      "TRADING-2399",
      "TRADING-2400",
      "TRADING-2401"
    ],
    "gap_category": "REPORTING_AND_ARTIFACT_NORMALIZATION",
    "gap_description": "historical candidate evidence exists but is distributed across task-specific schemas",
    "gap_id": "2402-REPORTING-01",
    "likely_impact": "owner review is slower and threshold calibration remains harder to audit",
    "owner_review_required": false,
    "recommended_fix": "define normalized dynamic strategy candidate evidence matrix",
    "recommended_next_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
    "severity": "MINOR"
  }
]

## Recommended next direction

{
  "decision_options": {
    "OPTION_A_FIX_DATA_QUALITY_WARNINGS_FIRST": {
      "meaning": "先处理 validate-data warnings 和 cached data coverage gap",
      "recommended": false
    },
    "OPTION_B_BUILD_PIT_COVERAGE_MATRIX": {
      "meaning": "先把 feature / signal / advisory 的 PIT 安全性系统化",
      "recommended": true
    },
    "OPTION_C_REVIEW_SIGNAL_CONSTRUCTION_FRAMEWORK": {
      "meaning": "回到 signal 本身，审计 growth_tilt / valid_until / regime signal quality",
      "recommended": true
    },
    "OPTION_D_REBUILD_REGIME_EXPECTATION_SCORING": {
      "meaning": "替换粗糙 regime pass-rate，改成 regime expectation score",
      "recommended": true
    },
    "OPTION_E_BUILD_THRESHOLD_META_DATASET": {
      "meaning": "汇总历史 candidate x threshold x decision，校准 gate",
      "recommended": true
    },
    "OPTION_F_RESUME_STRATEGY_CANDIDATE_SEARCH": {
      "meaning": "继续候选策略搜索",
      "recommended": false
    }
  },
  "recommended_next_research_task": "TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review",
  "recommended_options": [
    "OPTION_B_BUILD_PIT_COVERAGE_MATRIX",
    "OPTION_C_REVIEW_SIGNAL_CONSTRUCTION_FRAMEWORK",
    "OPTION_E_BUILD_THRESHOLD_META_DATASET"
  ],
  "recommended_priority": {
    "P0": [
      "PIT coverage matrix",
      "signal construction quality review",
      "regime labeling expectation review"
    ],
    "P1": [
      "threshold meta-dataset",
      "data quality warning classification"
    ],
    "P2": [
      "resume candidate search"
    ]
  },
  "record_ready": true
}

## Explicit non-approval list

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
- `new_strategy_backtest`
- `new_trading_signal`
- `new_scoring`
- `resume_candidate_search`
