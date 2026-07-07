# Dynamic strategy PIT coverage gap review

- status：`DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`

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
