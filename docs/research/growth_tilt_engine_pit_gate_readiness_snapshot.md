# Growth tilt engine PIT gate readiness snapshot

## 结论摘要

- status：`GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_READY_WITH_BLOCKERS_UNRESOLVED`
- source feature count：`10`
- as-of ready count：`2`
- source traceability ready count：`2`
- validity dependency ready count：`2`
- PIT gate ready count：`0`
- contract ready count：`0`
- PIT gate blocked count：`10`
- blocked by source traceability count：`5`
- blocked by valid_until_window count：`1`
- next route：`TRADING-2416_Growth_Tilt_Engine_Remaining_Contract_Blocker_Remediation_Plan`

2415 只从 TRADING-2410～2414 prior artifacts 聚合 PIT gate readiness。本任务没有修复 source feature、没有实现 valid_until_window、没有补造 PIT evidence，因此 `growth_tilt_engine` 与 `valid_until_window` blocker 仍保持 unresolved / undowngraded。

## PIT Gate Readiness Matrix

```json
{
  "allowed_pit_gate_statuses": [
    "pit_gate_ready",
    "pit_gate_blocked_by_missing_as_of_semantics",
    "pit_gate_blocked_by_missing_source_traceability",
    "pit_gate_blocked_by_missing_validity_dependency",
    "pit_gate_blocked_by_valid_until_window",
    "pit_gate_blocked_by_missing_upstream_artifact",
    "pit_gate_blocked_by_ambiguous_source_boundary",
    "pit_gate_not_applicable_non_signal_feature",
    "pit_gate_unresolved"
  ],
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "matrix_rows": [
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "mapped_with_caveats",
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "production_effect": "none",
      "source_feature_id": "adjusted_prices",
      "source_feature_name": "adjusted_prices",
      "source_feature_type": "MARKET_DATA",
      "source_pit_gate_status_before_2415": null,
      "source_system": "cached_data_artifact",
      "source_traceability_status": "mapped_with_caveats",
      "source_validity_dependency_remediation_status": null,
      "upstream_artifact_or_registry_reference": "data/raw/prices_daily.csv adjusted close fields",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "not_applicable"
    },
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "mapped_with_caveats",
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "production_effect": "none",
      "source_feature_id": "returns",
      "source_feature_name": "returns",
      "source_feature_type": "TECHNICAL_FEATURES",
      "source_pit_gate_status_before_2415": null,
      "source_system": "derived_research_artifact",
      "source_traceability_status": "mapped_with_caveats",
      "source_validity_dependency_remediation_status": null,
      "upstream_artifact_or_registry_reference": "derived from adjusted prices",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "not_applicable"
    },
    {
      "as_of_semantics_status": "ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "blocked_unresolved",
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "production_effect": "none",
      "source_feature_id": "volatility_inputs",
      "source_feature_name": "volatility_inputs",
      "source_feature_type": "TECHNICAL_FEATURES",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "derived_research_artifact",
      "source_traceability_status": "not_ready",
      "source_validity_dependency_remediation_status": "validity_dependency_blocked_by_missing_source_traceability",
      "upstream_artifact_or_registry_reference": "rolling price-derived volatility features",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "blocked_unresolved",
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "production_effect": "none",
      "source_feature_id": "trend_features",
      "source_feature_name": "trend_features",
      "source_feature_type": "TECHNICAL_FEATURES",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "derived_research_artifact",
      "source_traceability_status": "not_ready",
      "source_validity_dependency_remediation_status": "validity_dependency_blocked_by_missing_source_traceability",
      "upstream_artifact_or_registry_reference": "historical price trend / momentum windows",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "blocked_unresolved",
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "production_effect": "none",
      "source_feature_id": "drawdown_features",
      "source_feature_name": "drawdown_features",
      "source_feature_type": "TECHNICAL_FEATURES",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "derived_research_artifact",
      "source_traceability_status": "not_ready",
      "source_validity_dependency_remediation_status": "validity_dependency_blocked_by_missing_source_traceability",
      "upstream_artifact_or_registry_reference": "historical drawdown windows",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "mapped_with_caveats",
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "production_effect": "none",
      "source_feature_id": "equal_risk_baseline_weights",
      "source_feature_name": "equal_risk_baseline_weights",
      "source_feature_type": "PORTFOLIO_STATE",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "governed_config",
      "source_traceability_status": "ready",
      "source_validity_dependency_remediation_status": "validity_dependency_remediated",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "ready"
    },
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "blocked_unresolved",
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "production_effect": "none",
      "source_feature_id": "target_vol_policy",
      "source_feature_name": "target_vol_policy",
      "source_feature_type": "SIGNAL_CONSTRUCTION_POLICY",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "governed_config",
      "source_traceability_status": "not_ready",
      "source_validity_dependency_remediation_status": "validity_dependency_blocked_by_missing_source_traceability",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "mapped_with_caveats",
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "production_effect": "none",
      "source_feature_id": "risk_on_trend_filter_context",
      "source_feature_name": "risk_on_trend_filter_context",
      "source_feature_type": "BEHAVIOR_GUARDRAIL_CONTEXT",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "governed_config",
      "source_traceability_status": "ready",
      "source_validity_dependency_remediation_status": "validity_dependency_remediated",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "ready"
    },
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "blocked_unresolved",
      "pit_gate_blocking_reason": "valid_until_window_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_valid_until_window",
      "production_effect": "none",
      "source_feature_id": "execution_signal_validity_policy",
      "source_feature_name": "execution_signal_validity_policy",
      "source_feature_type": "EXECUTION_SEMANTIC_DEPENDENCY",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "governed_config",
      "source_traceability_status": "mapped_with_caveats",
      "source_validity_dependency_remediation_status": "validity_dependency_blocked_by_valid_until_window",
      "upstream_artifact_or_registry_reference": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
      "valid_until_available": false,
      "valid_until_required": true,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "broker_action": "none",
      "contract_ready": false,
      "eligible_for_candidate_search": false,
      "eligible_for_observation": false,
      "eligible_for_paper_shadow": false,
      "eligible_for_production": false,
      "mapping_status": "blocked_unresolved",
      "pit_gate_blocking_reason": "upstream_artifact_or_source_snapshot_unresolved",
      "pit_gate_status": "pit_gate_blocked_by_missing_upstream_artifact",
      "production_effect": "none",
      "source_feature_id": "growth_tilt_engine_signal_artifact",
      "source_feature_name": "growth_tilt_engine_signal_artifact",
      "source_feature_type": "SIGNAL_ARTIFACT_CONTRACT",
      "source_pit_gate_status_before_2415": "blocked_pending_pit_evidence",
      "source_system": "missing_artifact",
      "source_traceability_status": "not_ready",
      "source_validity_dependency_remediation_status": "validity_dependency_blocked_by_missing_source_traceability",
      "upstream_artifact_or_registry_reference": "missing standalone growth_tilt_engine signal artifact",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    }
  ],
  "production_effect": "none",
  "row_count": 10,
  "schema_version": "growth_tilt_engine_pit_gate_readiness_matrix.v1",
  "source_tasks": [
    "TRADING-2410",
    "TRADING-2411",
    "TRADING-2412",
    "TRADING-2413",
    "TRADING-2414"
  ]
}
```

## Remaining Blockers

```json
{
  "blocked_by_source_traceability_count": 5,
  "blocked_by_valid_until_window_count": 1,
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_search_enabled": false,
  "contract_ready_count": 0,
  "engine_id": "growth_tilt_engine",
  "growth_tilt_engine_blocker_downgraded": false,
  "growth_tilt_engine_blocker_resolved": false,
  "input_gap_count": 7,
  "observation_enabled": false,
  "paper_shadow_enabled": false,
  "pit_gate_blocked_count": 10,
  "pit_gate_ready_count": 0,
  "pit_gate_status_counts": {
    "pit_gate_blocked_by_missing_as_of_semantics": 4,
    "pit_gate_blocked_by_missing_source_traceability": 4,
    "pit_gate_blocked_by_missing_upstream_artifact": 1,
    "pit_gate_blocked_by_valid_until_window": 1
  },
  "production_effect": "none",
  "production_enabled": false,
  "recommended_next_task": "TRADING-2416_Growth_Tilt_Engine_Remaining_Contract_Blocker_Remediation_Plan",
  "remaining_blocking_reasons": [
    "as_of_semantics_gaps_remain_after_2412",
    "source_traceability_gaps_remain_after_2413",
    "valid_until_window_blocker_not_remediated_in_2414",
    "pit_gate_evidence_not_completed_before_2415",
    "growth_tilt_engine_blocker_not_downgraded_in_2415"
  ],
  "schema_version": "growth_tilt_engine_pit_gate_remaining_blocker_summary.v1",
  "source_feature_count": 10,
  "valid_until_window_blocker_downgraded": false,
  "valid_until_window_blocker_resolved": false
}
```

## Data Quality Boundary

- data_quality_gate_executed：`False`
- data_quality_gate_reason：`NOT_APPLICABLE_PIT_GATE_READINESS_SNAPSHOT_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## Safety Boundary

- growth_tilt_engine_blocker_resolved：`False`
- growth_tilt_engine_blocker_downgraded：`False`
- valid_until_window_blocker_resolved：`False`
- valid_until_window_blocker_downgraded：`False`
- candidate_search_enabled：`False`
- observation_enabled：`False`
- paper_shadow_enabled：`False`
- production_enabled：`False`
- broker_enabled：`False`