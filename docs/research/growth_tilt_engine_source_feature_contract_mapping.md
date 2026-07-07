# Growth tilt engine source feature contract mapping

## 结论摘要

- status：`GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED`
- blocker under review：`growth_tilt_engine`
- 已知 source feature 数：`10`
- 未分类 feature 数：`0`
- contract-ready feature 数：`0`
- blocked / gap feature 数：`7`
- 下一路线：`TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan`

2410 只做 `growth_tilt_engine` source feature 到 contract requirement 的映射与缺口分类。它不修复 growth tilt engine、不生成新信号、不执行 replay validation、不清除或降级 blocker、不恢复 candidate search。

## Mapping Status Policy

```json
[
  "mapped_contract_ready",
  "mapped_with_caveats",
  "missing_as_of_semantics",
  "missing_source_traceability",
  "missing_validity_dependency",
  "ambiguous_source_feature",
  "excluded_non_signal_feature",
  "blocked_unresolved"
]
```

## Source Feature Contract Mapping

```json
{
  "blockers_downgraded": false,
  "blockers_resolved": false,
  "broker_action": "none",
  "contract_mapping_validation": {
    "blocked_or_gap_count": 7,
    "broker_action": "none",
    "contract_ready_count": 0,
    "error_count": 0,
    "errors": [],
    "feature_count": 10,
    "production_effect": "none",
    "schema_version": "growth_tilt_engine_source_feature_contract_mapping_validation.v1",
    "unclassified_feature_count": 0,
    "valid": true,
    "warning_count": 5,
    "warnings": [
      {
        "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
        "feature_id": "volatility_inputs",
        "message": "feature remains mapped only to a remediation gap",
        "severity": "WARNING"
      },
      {
        "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
        "feature_id": "trend_features",
        "message": "feature remains mapped only to a remediation gap",
        "severity": "WARNING"
      },
      {
        "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
        "feature_id": "drawdown_features",
        "message": "feature remains mapped only to a remediation gap",
        "severity": "WARNING"
      },
      {
        "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
        "feature_id": "equal_risk_baseline_weights",
        "message": "feature remains mapped only to a remediation gap",
        "severity": "WARNING"
      },
      {
        "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
        "feature_id": "target_vol_policy",
        "message": "feature remains mapped only to a remediation gap",
        "severity": "WARNING"
      }
    ]
  },
  "contract_schema_source": "source_feature_traceability_contract.v1",
  "engine_id": "growth_tilt_engine",
  "known_source_feature_count": 10,
  "mapping_rows": [
    {
      "as_of_semantics": "historical adjusted rows are as-of-gated; corporate action basis review pending",
      "blocking_reason_if_unresolved": [],
      "contract_payload": {
        "as_of_handling": "DERIVED_FROM_SOURCE_CUTOFF",
        "explicit_reason": "resolve TQQQ adjustment-ratio warning before promotion-quality ranking",
        "feature_family": "growth_tilt_engine",
        "feature_id": "adjusted_prices",
        "forward_window_used": false,
        "generated_at_handling": "DERIVED_FROM_PIPELINE_RUN",
        "lookback_window": "QQQ/TQQQ/SGOV return and drawdown features",
        "pit_confidence": "MEDIUM",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [],
        "severity": "MATERIAL",
        "source_config": "data/raw/prices_daily.csv adjusted close fields",
        "source_data": "data/raw/prices_daily.csv adjusted close fields"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "adjusted_prices",
      "feature_name": "adjusted_prices",
      "feature_type": "MARKET_DATA",
      "mapping_status": "mapped_with_caveats",
      "mapping_status_reasons": [
        "source feature validates against contract but PIT evidence still has caveats"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "resolve TQQQ adjustment-ratio warning before promotion-quality ranking",
      "source_snapshot_requirement": "record upstream artifact id, source_data_cutoff, generated_at, and checksum",
      "source_system": "cached_data_artifact",
      "traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "data/raw/prices_daily.csv adjusted close fields",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "derived from historical price rows; window boundary needs explicit matrix",
      "blocking_reason_if_unresolved": [],
      "contract_payload": {
        "as_of_handling": "EXPLICIT_AS_OF",
        "explicit_reason": "record feature window start/end and adjusted-price basis per candidate",
        "feature_family": "growth_tilt_engine",
        "feature_id": "returns",
        "forward_window_used": false,
        "generated_at_handling": "DERIVED_FROM_PIPELINE_RUN",
        "lookback_window": "growth tilt, static baseline comparison, gate metrics",
        "pit_confidence": "MEDIUM",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [],
        "severity": "MATERIAL",
        "source_config": "derived from adjusted prices",
        "source_data": "derived from adjusted prices"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "returns",
      "feature_name": "returns",
      "feature_type": "TECHNICAL_FEATURES",
      "mapping_status": "mapped_with_caveats",
      "mapping_status_reasons": [
        "source feature validates against contract but PIT evidence still has caveats"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "record feature window start/end and adjusted-price basis per candidate",
      "source_snapshot_requirement": "record upstream artifact id, source_data_cutoff, generated_at, and checksum",
      "source_system": "derived_research_artifact",
      "traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "derived from adjusted prices",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "rolling windows appear historical but explicit as-of lineage is missing",
      "blocking_reason_if_unresolved": [
        "as-of semantics are missing"
      ],
      "contract_payload": {
        "as_of_handling": "EXPLICIT_AS_OF",
        "explicit_reason": "add window end-date and no-forward-fill assertion to PIT matrix",
        "feature_family": "growth_tilt_engine",
        "feature_id": "volatility_inputs",
        "forward_window_used": false,
        "generated_at_handling": "APPROXIMATE",
        "lookback_window": "vol target / risk and regime diagnostics",
        "pit_confidence": "MEDIUM",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [
          "MISSING_DATA_RISK"
        ],
        "severity": "MATERIAL",
        "source_config": "rolling price-derived volatility features",
        "source_data": "rolling price-derived volatility features"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "volatility_inputs",
      "feature_name": "volatility_inputs",
      "feature_type": "TECHNICAL_FEATURES",
      "mapping_status": "missing_as_of_semantics",
      "mapping_status_reasons": [
        "as-of semantics are missing"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "add window end-date and no-forward-fill assertion to PIT matrix",
      "source_snapshot_requirement": "record upstream artifact id, source_data_cutoff, generated_at, and checksum",
      "source_system": "derived_research_artifact",
      "traceability_status": "missing",
      "upstream_artifact_or_registry_reference": "rolling price-derived volatility features",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "historical windows likely PIT, but feature-level lineage is not explicit",
      "blocking_reason_if_unresolved": [
        "source traceability or generated_at lineage is missing"
      ],
      "contract_payload": {
        "as_of_handling": "EXPLICIT_AS_OF",
        "explicit_reason": "make trend feature windows explicit before observation review",
        "feature_family": "growth_tilt_engine",
        "feature_id": "trend_features",
        "forward_window_used": false,
        "generated_at_handling": "APPROXIMATE",
        "lookback_window": "growth tilt and regime labels",
        "pit_confidence": "MEDIUM",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [],
        "severity": "MATERIAL",
        "source_config": "historical price trend / momentum windows",
        "source_data": "historical price trend / momentum windows"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "trend_features",
      "feature_name": "trend_features",
      "feature_type": "TECHNICAL_FEATURES",
      "mapping_status": "missing_source_traceability",
      "mapping_status_reasons": [
        "source traceability or generated_at lineage is missing"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "make trend feature windows explicit before observation review",
      "source_snapshot_requirement": "record upstream artifact id, source_data_cutoff, generated_at, and checksum",
      "source_system": "derived_research_artifact",
      "traceability_status": "partial",
      "upstream_artifact_or_registry_reference": "historical price trend / momentum windows",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "research artifacts use drawdown evidence but PIT field lineage is missing",
      "blocking_reason_if_unresolved": [
        "as-of semantics are missing"
      ],
      "contract_payload": {
        "as_of_handling": "UNKNOWN",
        "explicit_reason": "separate live-available drawdown inputs from ex-post evaluation metrics",
        "feature_family": "growth_tilt_engine",
        "feature_id": "drawdown_features",
        "forward_window_used": false,
        "generated_at_handling": "APPROXIMATE",
        "lookback_window": "drawdown materiality gate and risk-off evidence",
        "pit_confidence": "MEDIUM",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [
          "MISSING_DATA_RISK"
        ],
        "severity": "MATERIAL",
        "source_config": "historical drawdown windows",
        "source_data": "historical drawdown windows"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "drawdown_features",
      "feature_name": "drawdown_features",
      "feature_type": "TECHNICAL_FEATURES",
      "mapping_status": "missing_as_of_semantics",
      "mapping_status_reasons": [
        "as-of semantics are missing"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "separate live-available drawdown inputs from ex-post evaluation metrics",
      "source_snapshot_requirement": "record upstream artifact id, source_data_cutoff, generated_at, and checksum",
      "source_system": "derived_research_artifact",
      "traceability_status": "missing",
      "upstream_artifact_or_registry_reference": "historical drawdown windows",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "derived from trailing QQQ/SGOV realized volatility; no standalone as-of contract",
      "blocking_reason_if_unresolved": [
        "source traceability or generated_at lineage is missing"
      ],
      "contract_payload": {
        "as_of_handling": "DERIVED_FROM_SOURCE_CUTOFF",
        "explicit_reason": "Emit baseline weight inputs with as_of_date and source_data_cutoff.",
        "feature_family": "growth_tilt_engine",
        "feature_id": "equal_risk_baseline_weights",
        "forward_window_used": false,
        "generated_at_handling": "DERIVED_FROM_PIPELINE_RUN",
        "lookback_window": "medium_realized_vol=60",
        "pit_confidence": "MEDIUM",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [
          "BACKFILL_RISK"
        ],
        "severity": "MATERIAL",
        "source_config": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
        "source_data": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "equal_risk_baseline_weights",
      "feature_name": "equal_risk_baseline_weights",
      "feature_type": "PORTFOLIO_STATE",
      "mapping_status": "missing_source_traceability",
      "mapping_status_reasons": [
        "source traceability or generated_at lineage is missing"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "Emit baseline weight inputs with as_of_date and source_data_cutoff.",
      "source_snapshot_requirement": "record config path, policy version, checksum, and generated_at",
      "source_system": "governed_config",
      "traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "target-vol weights use trailing volatility in code but lack explicit as-of metadata",
      "blocking_reason_if_unresolved": [
        "source traceability or generated_at lineage is missing"
      ],
      "contract_payload": {
        "as_of_handling": "EXPLICIT_AS_OF",
        "explicit_reason": "Define signal_horizon_days and record target-vol input window boundaries.",
        "feature_family": "growth_tilt_engine",
        "feature_id": "target_vol_policy",
        "forward_window_used": false,
        "generated_at_handling": "UNKNOWN",
        "lookback_window": "20,60,120",
        "pit_confidence": "MEDIUM",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [
          "BACKFILL_RISK"
        ],
        "severity": "MATERIAL",
        "source_config": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
        "source_data": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "target_vol_policy",
      "feature_name": "target_vol_policy",
      "feature_type": "SIGNAL_CONSTRUCTION_POLICY",
      "mapping_status": "missing_source_traceability",
      "mapping_status_reasons": [
        "source traceability or generated_at lineage is missing"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "Define signal_horizon_days and record target-vol input window boundaries.",
      "source_snapshot_requirement": "record config path, policy version, checksum, and generated_at",
      "source_system": "governed_config",
      "traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "trend / volatility / drawdown thresholds are trailing by design but not emitted per signal",
      "blocking_reason_if_unresolved": [],
      "contract_payload": {
        "as_of_handling": "DERIVED_FROM_SOURCE_CUTOFF",
        "explicit_reason": "Separate ex-ante risk-on conditions from ex-post regime labels.",
        "feature_family": "growth_tilt_engine",
        "feature_id": "risk_on_trend_filter_context",
        "forward_window_used": false,
        "generated_at_handling": "UNKNOWN",
        "lookback_window": {
          "moving_average_windows": {
            "long": 200,
            "short": 100
          },
          "realized_vol_percentile_window": 252
        },
        "pit_confidence": "LOW",
        "pit_status": "APPROXIMATE_PIT",
        "risk_flags": [
          "REGIME_CONFIRMATION_RISK",
          "THRESHOLD_UNCALIBRATED"
        ],
        "severity": "MATERIAL",
        "source_config": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
        "source_data": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "risk_on_trend_filter_context",
      "feature_name": "risk_on_trend_filter_context",
      "feature_type": "BEHAVIOR_GUARDRAIL_CONTEXT",
      "mapping_status": "mapped_with_caveats",
      "mapping_status_reasons": [
        "source feature validates against contract but PIT evidence still has caveats"
      ],
      "pit_eligibility": "APPROXIMATE_PIT",
      "recommended_action": "Separate ex-ante risk-on conditions from ex-post regime labels.",
      "source_snapshot_requirement": "record config path, policy version, checksum, and generated_at",
      "source_system": "governed_config",
      "traceability_status": "missing",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    },
    {
      "as_of_semantics": "after_market_close",
      "blocking_reason_if_unresolved": [
        "execution_signal_validity_policy remains BLOCKING in 2406 inventory"
      ],
      "contract_payload": {
        "as_of_handling": "EXPLICIT_AS_OF",
        "explicit_reason": "Route valid-from / valid-until semantics to TRADING-2407.",
        "feature_family": "growth_tilt_engine",
        "feature_id": "execution_signal_validity_policy",
        "forward_window_used": true,
        "generated_at_handling": "APPROXIMATE",
        "lookback_window": "not applicable",
        "pit_confidence": "LOW",
        "pit_status": "UNKNOWN",
        "risk_flags": [
          "STALE_DATA_RISK",
          "VALID_UNTIL_UNGROUNDED"
        ],
        "severity": "BLOCKING",
        "source_config": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
        "source_data": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "execution_signal_validity_policy",
      "feature_name": "execution_signal_validity_policy",
      "feature_type": "EXECUTION_SEMANTIC_DEPENDENCY",
      "mapping_status": "blocked_unresolved",
      "mapping_status_reasons": [
        "execution_signal_validity_policy remains BLOCKING in 2406 inventory"
      ],
      "pit_eligibility": "UNKNOWN_OR_APPROXIMATE_PIT",
      "recommended_action": "Route valid-from / valid-until semantics to TRADING-2407.",
      "source_snapshot_requirement": "record config path, policy version, checksum, and generated_at",
      "source_system": "governed_config",
      "traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "depends_on_valid_until_window_contract"
    },
    {
      "as_of_semantics": "missing as_of_date field",
      "blocking_reason_if_unresolved": [
        "growth_tilt_engine_signal_artifact remains BLOCKING in 2406 inventory"
      ],
      "contract_payload": {
        "as_of_handling": "UNKNOWN",
        "explicit_reason": "Implement explicit as-of contract before downgrading blocker.",
        "feature_family": "growth_tilt_engine",
        "feature_id": "growth_tilt_engine_signal_artifact",
        "forward_window_used": true,
        "generated_at_handling": "UNKNOWN",
        "lookback_window": "TBD_FROM_SIGNAL_IMPLEMENTATION",
        "pit_confidence": "LOW",
        "pit_status": "UNKNOWN",
        "risk_flags": [
          "LOOKAHEAD_RISK",
          "BACKFILL_RISK",
          "STALE_DATA_RISK",
          "MISSING_DATA_RISK"
        ],
        "severity": "BLOCKING",
        "source_config": "missing standalone growth_tilt_engine signal artifact",
        "source_data": "missing standalone growth_tilt_engine signal artifact"
      },
      "contract_validation_result": {
        "error_count": 0,
        "errors": [],
        "schema_name": "source_feature_traceability_contract",
        "valid": true,
        "warning_count": 0,
        "warnings": []
      },
      "feature_id": "growth_tilt_engine_signal_artifact",
      "feature_name": "growth_tilt_engine_signal_artifact",
      "feature_type": "SIGNAL_ARTIFACT_CONTRACT",
      "mapping_status": "blocked_unresolved",
      "mapping_status_reasons": [
        "growth_tilt_engine_signal_artifact remains BLOCKING in 2406 inventory"
      ],
      "pit_eligibility": "UNKNOWN_OR_APPROXIMATE_PIT",
      "recommended_action": "Implement explicit as-of contract before downgrading blocker.",
      "source_snapshot_requirement": "record upstream artifact id, source_data_cutoff, generated_at, and checksum",
      "source_system": "missing_artifact",
      "traceability_status": "missing",
      "upstream_artifact_or_registry_reference": "missing standalone growth_tilt_engine signal artifact",
      "used_by_growth_tilt_engine": true,
      "validity_dependency": "none_identified_in_2410"
    }
  ],
  "mapping_statuses_allowed": [
    "mapped_contract_ready",
    "mapped_with_caveats",
    "missing_as_of_semantics",
    "missing_source_traceability",
    "missing_validity_dependency",
    "ambiguous_source_feature",
    "excluded_non_signal_feature",
    "blocked_unresolved"
  ],
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_source_feature_contract_mapping.v1",
  "unresolved_gap_summary": {
    "broker_action": "none",
    "broker_enabled": false,
    "candidate_search_enabled": false,
    "engine_id": "growth_tilt_engine",
    "growth_tilt_engine_blocking_gap_resolved": false,
    "growth_tilt_engine_severity_downgraded": false,
    "observation_enabled": false,
    "paper_shadow_enabled": false,
    "production_effect": "none",
    "production_enabled": false,
    "recommended_next_task": "TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan",
    "schema_version": "growth_tilt_engine_contract_gap_summary.v1",
    "status_counts": {
      "ambiguous_source_feature": 0,
      "blocked_unresolved": 2,
      "excluded_non_signal_feature": 0,
      "mapped_contract_ready": 0,
      "mapped_with_caveats": 3,
      "missing_as_of_semantics": 2,
      "missing_source_traceability": 3,
      "missing_validity_dependency": 0
    },
    "unresolved_feature_ids": [
      "volatility_inputs",
      "trend_features",
      "drawdown_features",
      "equal_risk_baseline_weights",
      "target_vol_policy",
      "execution_signal_validity_policy",
      "growth_tilt_engine_signal_artifact"
    ]
  }
}
```

## Contract Mapping Validation

```json
{
  "blocked_or_gap_count": 7,
  "broker_action": "none",
  "contract_ready_count": 0,
  "error_count": 0,
  "errors": [],
  "feature_count": 10,
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_source_feature_contract_mapping_validation.v1",
  "unclassified_feature_count": 0,
  "valid": true,
  "warning_count": 5,
  "warnings": [
    {
      "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
      "feature_id": "volatility_inputs",
      "message": "feature remains mapped only to a remediation gap",
      "severity": "WARNING"
    },
    {
      "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
      "feature_id": "trend_features",
      "message": "feature remains mapped only to a remediation gap",
      "severity": "WARNING"
    },
    {
      "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
      "feature_id": "drawdown_features",
      "message": "feature remains mapped only to a remediation gap",
      "severity": "WARNING"
    },
    {
      "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
      "feature_id": "equal_risk_baseline_weights",
      "message": "feature remains mapped only to a remediation gap",
      "severity": "WARNING"
    },
    {
      "code": "MAPPING_GAP_REQUIRES_2411_REMEDIATION",
      "feature_id": "target_vol_policy",
      "message": "feature remains mapped only to a remediation gap",
      "severity": "WARNING"
    }
  ]
}
```

## Unresolved Gap Summary

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_search_enabled": false,
  "engine_id": "growth_tilt_engine",
  "growth_tilt_engine_blocking_gap_resolved": false,
  "growth_tilt_engine_severity_downgraded": false,
  "observation_enabled": false,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "recommended_next_task": "TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan",
  "schema_version": "growth_tilt_engine_contract_gap_summary.v1",
  "status_counts": {
    "ambiguous_source_feature": 0,
    "blocked_unresolved": 2,
    "excluded_non_signal_feature": 0,
    "mapped_contract_ready": 0,
    "mapped_with_caveats": 3,
    "missing_as_of_semantics": 2,
    "missing_source_traceability": 3,
    "missing_validity_dependency": 0
  },
  "unresolved_feature_ids": [
    "volatility_inputs",
    "trend_features",
    "drawdown_features",
    "equal_risk_baseline_weights",
    "target_vol_policy",
    "execution_signal_validity_policy",
    "growth_tilt_engine_signal_artifact"
  ]
}
```

## Data Quality Boundary

- data_quality_gate_executed：`False`
- data_quality_gate_reason：`NOT_APPLICABLE_SOURCE_FEATURE_CONTRACT_MAPPING_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## Safety Boundary

- blockers_resolved：`False`
- blockers_downgraded：`False`
- growth_tilt_engine_blocking_gap_resolved：`False`
- candidate_search_enabled：`False`
- observation_enabled：`False`
- paper_shadow_enabled：`False`
- production_enabled：`False`
- broker_enabled：`False`