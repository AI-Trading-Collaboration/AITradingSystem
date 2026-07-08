# Growth tilt engine PIT gate remaining blocker closure plan

## Executive summary

- status：`GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY`
- source feature count：`10`
- PIT gate ready count：`0`
- contract ready count：`0`
- PIT gate blocked count：`10`
- blocked by source traceability count：`5`
- blocked by valid_until_window count：`1`
- recommended next route：`TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure`

TRADING-2415 没有说明 blocker 已解除。相反，2415 确认全部 10 个 source features 仍未达到 PIT gate ready / contract ready。2416 只把这些 remaining blockers 拆成 closure plan，为 2417 的 source traceability / upstream artifact closure 做准备。

## Source findings from TRADING-2415

```json
{
  "blocked_by_source_traceability_count": 5,
  "blocked_by_valid_until_window_count": 1,
  "blockers_unresolved": true,
  "contract_ready_count": 0,
  "pit_gate_blocked_count": 10,
  "pit_gate_ready_count": 0,
  "source_feature_count": 10
}
```

## Current readiness snapshot interpretation

```json
{
  "blocker_downgrade_allowed": false,
  "broker_action": "none",
  "candidate_search_allowed": false,
  "contract_ready": false,
  "observation_allowed": false,
  "pit_gate_ready": false,
  "production_effect": "none"
}
```

## Remaining blocker matrix

```json
{
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "matrix_rows": [
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": false,
      "blocked_by_source_traceability": false,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "feature_family": "market_data",
      "feature_id": "adjusted_prices",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "cached_data_artifact",
      "source_traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "data/raw/prices_daily.csv adjusted close fields",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "not_applicable"
    },
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": false,
      "blocked_by_source_traceability": false,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "feature_family": "derived_price_feature",
      "feature_id": "returns",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "derived_research_artifact",
      "source_traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "derived from adjusted prices",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "not_applicable"
    },
    {
      "as_of_semantics_status": "ready",
      "blocked_by_as_of_contract": false,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": true,
      "blocked_by_source_traceability": true,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "SOURCE_TRACEABILITY_GAP",
        "SIGNAL_VALIDITY_DEPENDENCY_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "feature_family": "derived_price_feature",
      "feature_id": "volatility_inputs",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "source_config_id",
        "source_artifact_id",
        "owner_module",
        "signal_version",
        "signal_validity_contract",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "derived_research_artifact",
      "source_traceability_status": "not_ready",
      "upstream_artifact_or_registry_reference": "rolling price-derived volatility features",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": true,
      "blocked_by_source_traceability": true,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "SOURCE_TRACEABILITY_GAP",
        "SIGNAL_VALIDITY_DEPENDENCY_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "feature_family": "derived_price_feature",
      "feature_id": "trend_features",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "source_config_id",
        "source_artifact_id",
        "owner_module",
        "signal_version",
        "signal_validity_contract",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "derived_research_artifact",
      "source_traceability_status": "not_ready",
      "upstream_artifact_or_registry_reference": "historical price trend / momentum windows",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "ready",
      "blocked_by_as_of_contract": false,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": true,
      "blocked_by_source_traceability": true,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "SOURCE_TRACEABILITY_GAP",
        "SIGNAL_VALIDITY_DEPENDENCY_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "feature_family": "derived_price_feature",
      "feature_id": "drawdown_features",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "source_config_id",
        "source_artifact_id",
        "owner_module",
        "signal_version",
        "signal_validity_contract",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "derived_research_artifact",
      "source_traceability_status": "not_ready",
      "upstream_artifact_or_registry_reference": "historical drawdown windows",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": false,
      "blocked_by_source_traceability": false,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "feature_family": "portfolio_state",
      "feature_id": "equal_risk_baseline_weights",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "governed_config",
      "source_traceability_status": "ready",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "ready"
    },
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": true,
      "blocked_by_source_traceability": true,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "SOURCE_TRACEABILITY_GAP",
        "SIGNAL_VALIDITY_DEPENDENCY_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_source_traceability",
      "feature_family": "signal_construction_policy",
      "feature_id": "target_vol_policy",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "source_traceability_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "source_config_id",
        "source_artifact_id",
        "owner_module",
        "signal_version",
        "signal_validity_contract",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "governed_config",
      "source_traceability_status": "not_ready",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": false,
      "blocked_by_source_traceability": false,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_as_of_semantics",
      "feature_family": "behavior_guardrail_context",
      "feature_id": "risk_on_trend_filter_context",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "as_of_semantics_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "governed_config",
      "source_traceability_status": "ready",
      "upstream_artifact_or_registry_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "ready"
    },
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": true,
      "blocked_by_source_traceability": false,
      "blocked_by_upstream_artifact": false,
      "blocked_by_valid_until_window": true,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "VALID_UNTIL_DEPENDENCY_GAP",
        "SIGNAL_VALIDITY_DEPENDENCY_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_valid_until_window",
      "feature_family": "execution_semantic_dependency",
      "feature_id": "execution_signal_validity_policy",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "valid_until_window_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2418_Valid_Until_Window_Dependency_Evidence_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "valid_from",
        "valid_until",
        "stale_signal_policy",
        "signal_version",
        "signal_validity_contract",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "governed_config",
      "source_traceability_status": "mapped_with_caveats",
      "upstream_artifact_or_registry_reference": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
      "valid_until_available": false,
      "valid_until_required": true,
      "validity_dependency_status": "blocked"
    },
    {
      "as_of_semantics_status": "not_ready",
      "blocked_by_as_of_contract": true,
      "blocked_by_missing_evidence": true,
      "blocked_by_signal_validity_dependency": true,
      "blocked_by_source_traceability": true,
      "blocked_by_upstream_artifact": true,
      "blocked_by_valid_until_window": false,
      "blocker_categories": [
        "AS_OF_CONTRACT_GAP",
        "SOURCE_TRACEABILITY_GAP",
        "UPSTREAM_ARTIFACT_GAP",
        "SIGNAL_VALIDITY_DEPENDENCY_GAP",
        "PIT_GATE_EVIDENCE_GAP"
      ],
      "broker_action": "none",
      "current_contract_status": "blocked",
      "current_pit_gate_status": "pit_gate_blocked_by_missing_upstream_artifact",
      "feature_family": "signal_artifact_contract",
      "feature_id": "growth_tilt_engine_signal_artifact",
      "owner_review_required": true,
      "pit_gate_blocking_reason": "upstream_artifact_or_source_snapshot_unresolved",
      "production_effect": "none",
      "recommended_closure_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_closure_evidence": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "source_config_id",
        "source_artifact_id",
        "owner_module",
        "artifact_row_count",
        "artifact_checksum",
        "signal_version",
        "signal_validity_contract",
        "feature_version",
        "PIT_gate_checker_regenerated"
      ],
      "severity": "BLOCKING",
      "source_system": "missing_artifact",
      "source_traceability_status": "not_ready",
      "upstream_artifact_or_registry_reference": "missing standalone growth_tilt_engine signal artifact",
      "valid_until_available": false,
      "valid_until_required": false,
      "validity_dependency_status": "blocked"
    }
  ],
  "production_effect": "none",
  "row_count": 10,
  "schema_version": "growth_tilt_engine_pit_gate_remaining_blocker_matrix.v1",
  "source_tasks": [
    "TRADING-2410",
    "TRADING-2411",
    "TRADING-2412",
    "TRADING-2413",
    "TRADING-2414",
    "TRADING-2415"
  ]
}
```

## Source traceability closure plan

```json
{
  "broker_action": "none",
  "closure_rows": [
    {
      "broker_action": "none",
      "expected_evidence_after_fix": [
        "source_config_id_or_source_artifact_id",
        "owner_module",
        "generated_at",
        "source_data_cutoff_if_data_derived",
        "feature_version",
        "source_feature_ids",
        "artifact_row_count_and_checksum_where_practical"
      ],
      "feature_id": "volatility_inputs",
      "missing_feature_version": true,
      "missing_generated_at": true,
      "missing_owner_module": true,
      "missing_source_artifact": true,
      "missing_source_config": false,
      "missing_source_data_cutoff": true,
      "production_effect": "none",
      "recommended_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_fix": "Bind the source feature to a governed source config or upstream artifact with generated_at, cutoff, owner module, version and checksum evidence."
    },
    {
      "broker_action": "none",
      "expected_evidence_after_fix": [
        "source_config_id_or_source_artifact_id",
        "owner_module",
        "generated_at",
        "source_data_cutoff_if_data_derived",
        "feature_version",
        "source_feature_ids",
        "artifact_row_count_and_checksum_where_practical"
      ],
      "feature_id": "trend_features",
      "missing_feature_version": true,
      "missing_generated_at": true,
      "missing_owner_module": true,
      "missing_source_artifact": true,
      "missing_source_config": false,
      "missing_source_data_cutoff": true,
      "production_effect": "none",
      "recommended_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_fix": "Bind the source feature to a governed source config or upstream artifact with generated_at, cutoff, owner module, version and checksum evidence."
    },
    {
      "broker_action": "none",
      "expected_evidence_after_fix": [
        "source_config_id_or_source_artifact_id",
        "owner_module",
        "generated_at",
        "source_data_cutoff_if_data_derived",
        "feature_version",
        "source_feature_ids",
        "artifact_row_count_and_checksum_where_practical"
      ],
      "feature_id": "drawdown_features",
      "missing_feature_version": true,
      "missing_generated_at": true,
      "missing_owner_module": true,
      "missing_source_artifact": true,
      "missing_source_config": false,
      "missing_source_data_cutoff": true,
      "production_effect": "none",
      "recommended_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_fix": "Bind the source feature to a governed source config or upstream artifact with generated_at, cutoff, owner module, version and checksum evidence."
    },
    {
      "broker_action": "none",
      "expected_evidence_after_fix": [
        "source_config_id_or_source_artifact_id",
        "owner_module",
        "generated_at",
        "source_data_cutoff_if_data_derived",
        "feature_version",
        "source_feature_ids",
        "artifact_row_count_and_checksum_where_practical"
      ],
      "feature_id": "target_vol_policy",
      "missing_feature_version": true,
      "missing_generated_at": true,
      "missing_owner_module": false,
      "missing_source_artifact": false,
      "missing_source_config": false,
      "missing_source_data_cutoff": false,
      "production_effect": "none",
      "recommended_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_fix": "Bind the source feature to a governed source config or upstream artifact with generated_at, cutoff, owner module, version and checksum evidence."
    },
    {
      "broker_action": "none",
      "expected_evidence_after_fix": [
        "source_config_id_or_source_artifact_id",
        "owner_module",
        "generated_at",
        "source_data_cutoff_if_data_derived",
        "feature_version",
        "source_feature_ids",
        "artifact_row_count_and_checksum_where_practical"
      ],
      "feature_id": "growth_tilt_engine_signal_artifact",
      "missing_feature_version": true,
      "missing_generated_at": true,
      "missing_owner_module": true,
      "missing_source_artifact": true,
      "missing_source_config": true,
      "missing_source_data_cutoff": true,
      "production_effect": "none",
      "recommended_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "required_fix": "Bind the source feature to a governed source config or upstream artifact with generated_at, cutoff, owner module, version and checksum evidence."
    }
  ],
  "production_effect": "none",
  "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
  "schema_version": "growth_tilt_engine_source_traceability_closure_plan.v1",
  "source_traceability_gap_count": 5
}
```

## As-of evidence closure plan

```json
{
  "as_of_contract_gap_count": 8,
  "broker_action": "none",
  "closure_rows": [
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "adjusted_prices",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "requires_source_traceability_before_recheck": false,
      "requires_valid_until_before_recheck": false
    },
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "returns",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "requires_source_traceability_before_recheck": false,
      "requires_valid_until_before_recheck": false
    },
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "trend_features",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "source_artifact_id",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "requires_source_traceability_before_recheck": true,
      "requires_valid_until_before_recheck": false
    },
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "equal_risk_baseline_weights",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "source_config_id",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "requires_source_traceability_before_recheck": false,
      "requires_valid_until_before_recheck": false
    },
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "target_vol_policy",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "signal_version",
        "source_artifact_id",
        "source_config_id",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "requires_source_traceability_before_recheck": true,
      "requires_valid_until_before_recheck": false
    },
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "risk_on_trend_filter_context",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "source_config_id",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "requires_source_traceability_before_recheck": false,
      "requires_valid_until_before_recheck": false
    },
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "execution_signal_validity_policy",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "signal_version",
        "source_config_id",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2418_Valid_Until_Window_Dependency_Evidence_Closure",
      "requires_source_traceability_before_recheck": false,
      "requires_valid_until_before_recheck": true
    },
    {
      "broker_action": "none",
      "current_as_of_semantics_status": "not_ready",
      "feature_id": "growth_tilt_engine_signal_artifact",
      "missing_required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "feature_version",
        "signal_version",
        "source_artifact_id",
        "source_feature_ids"
      ],
      "production_effect": "none",
      "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
      "requires_source_traceability_before_recheck": true,
      "requires_valid_until_before_recheck": false
    }
  ],
  "production_effect": "none",
  "recommended_next_task": "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure",
  "required_fields": [
    "as_of_date",
    "generated_at",
    "source_data_cutoff",
    "signal_version",
    "feature_version",
    "source_artifact_id",
    "source_config_id",
    "source_feature_ids"
  ],
  "required_fields_missing_count": 50,
  "schema_version": "growth_tilt_engine_as_of_evidence_closure_plan.v1"
}
```

## Valid-until dependency closure plan

```json
{
  "broker_action": "none",
  "dependent_feature_ids": [
    "execution_signal_validity_policy"
  ],
  "dependent_feature_or_signal_count": 1,
  "pit_input_registry_candidate_search_blocker": true,
  "pit_input_registry_severity": "BLOCKING",
  "production_effect": "none",
  "recommended_later_task": "TRADING-2418_Valid_Until_Window_Dependency_Evidence_Closure",
  "requires_signal_validity_contract_evidence": true,
  "requires_stale_signal_policy_evidence": true,
  "requires_valid_from_valid_until_mapping": true,
  "schema_version": "growth_tilt_engine_valid_until_dependency_closure_plan.v1",
  "valid_until_window_still_blocking": true
}
```

## PIT gate evidence requirements

```json
{
  "auto_downgrade_allowed": false,
  "blocker_downgrade_requires_owner_review": true,
  "broker_action": "none",
  "candidate_search_allowed_before_recheck": false,
  "pit_gate_ready_requires": [
    "source_traceability_complete",
    "as_of_contract_fields_complete",
    "source_data_cutoff_available",
    "no_unexplained_forward_window_dependency",
    "upstream_artifact_available",
    "feature_version_available",
    "signal_or_feature_owner_module_known",
    "valid_until_dependency_resolved_if_applicable",
    "PIT_gate_checker_regenerated",
    "owner_review_recorded_if_downgrade_required"
  ],
  "production_effect": "none",
  "recommended_owner_review_task": "TRADING-2420_Growth_Tilt_Engine_PIT_Blocker_Downgrade_Owner_Review",
  "recommended_recheck_task": "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck",
  "schema_version": "growth_tilt_engine_pit_gate_evidence_requirements.v1"
}
```

## Proposed 2417-2420 sequence

```json
{
  "TRADING-2417": {
    "does_not": [
      "downgrade_blocker",
      "resume_candidate_search"
    ],
    "purpose": "close remaining source traceability / upstream artifact gaps",
    "title": "Growth Tilt Engine Source Traceability And Upstream Artifact Closure"
  },
  "TRADING-2418": {
    "does_not": [
      "approve_observation"
    ],
    "purpose": "close valid_until_window-dependent readiness gaps",
    "title": "Valid Until Window Dependency Evidence Closure"
  },
  "TRADING-2419": {
    "does_not": [
      "auto_downgrade"
    ],
    "purpose": "regenerate readiness snapshot after closure evidence",
    "title": "Growth Tilt Engine PIT Gate Readiness Recheck"
  },
  "TRADING-2420": {
    "does_not": [
      "approve_paper_shadow"
    ],
    "purpose": "owner review before any severity downgrade",
    "title": "Growth Tilt Engine PIT Blocker Downgrade Owner Review"
  }
}
```

## Explicit non-approval list

```json
[
  "mark_any_source_feature_pit_gate_ready",
  "mark_any_source_feature_contract_ready",
  "downgrade_growth_tilt_engine_blocker",
  "downgrade_valid_until_window_blocker",
  "clear_any_blocking_gap",
  "resume_candidate_search",
  "approve_research_only_observation",
  "enable_paper_shadow",
  "create_paper_trade",
  "create_shadow_position",
  "enable_scheduler",
  "append_historical_event_log",
  "bind_outcome",
  "mutate_outcome_store",
  "enable_production",
  "call_broker_api",
  "send_order",
  "create_scheduled_task",
  "generate_daily_report",
  "run_new_strategy_backtest",
  "generate_new_trading_signal",
  "run_scoring"
]
```

## Recommended next route

2417 应优先处理 source traceability / upstream artifact closure，因为 2415 中最大的明确 blocker group 是 source traceability：5 个 source features 被其阻塞。在这些 evidence 未补齐前，as-of replay、PIT gate readiness 和 blocker downgrade owner review 都缺证据基础。

## Data quality boundary

- data_quality_gate_executed：`False`
- data_quality_gate_reason：`NOT_APPLICABLE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## Safety boundary

- growth_tilt_engine_blocking_gap_resolved：`False`
- growth_tilt_engine_severity_downgraded：`False`
- valid_until_window_blocking_gap_resolved：`False`
- valid_until_window_severity_downgraded：`False`
- candidate_search_resumed：`False`
- research_only_observation_approved：`False`
- paper_shadow_enabled：`False`
- production_enabled：`False`
- broker_action_enabled：`False`
- daily_report_generated：`False`