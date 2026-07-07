# Growth tilt engine source traceability remediation

## 结论摘要

- status：`GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`
- input gap count：`7`
- source traceability gap count：`7`
- source traceability remediated count：`2`
- remaining source traceability gap count：`5`
- remaining blocked or gap count：`7`
- contract ready count：`0`
- next route：`TRADING-2414_Growth_Tilt_Engine_Signal_Validity_Dependency_Remediation`

2413 只补齐 source traceability metadata。signal validity dependency、PIT gate 和 valid_until_window 仍未在本任务中修复，因此 `growth_tilt_engine` blocker 不能解除或降级。

## Before / After

```json
{
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "production_effect": "none",
  "record_count": 7,
  "records": [
    {
      "after": {
        "as_of_remediation_status": null,
        "as_of_semantics_status": null,
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "as_of_semantics_status",
          "validity_dependency_status",
          "pit_gate_status"
        ],
        "feature_id": "equal_risk_baseline_weights",
        "mapping_status": "mapped_with_caveats",
        "mapping_status_reason": "source traceability remediated in TRADING-2413; validity dependency and PIT gate evidence remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "equal_risk_baseline_weights",
        "source_traceability_contract_metadata": {
          "as_of_semantics_status": null,
          "broker_action": "none",
          "contract_ready": false,
          "derived_from_prior_artifact": false,
          "fresh_market_data_required": false,
          "pit_gate_status": "blocked_pending_pit_evidence",
          "production_effect": "none",
          "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
          "source_feature_id": "equal_risk_baseline_weights",
          "source_feature_name": "equal_risk_baseline_weights",
          "source_snapshot_hash": "sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
          "source_snapshot_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk@sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
          "source_traceability_contract_id": "growth_tilt_engine:equal_risk_baseline_weights:source_traceability:v1",
          "source_traceability_remediation_status": "source_traceability_remediated",
          "traceability_blocking_reason": null,
          "traceability_status": "ready",
          "upstream_artifact_id": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
          "upstream_artifact_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
          "upstream_config_key": "research_policy.equal_risk",
          "upstream_config_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
          "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
          "upstream_source_system": "governed_config",
          "validity_dependency_status": "not_assessed_in_2413"
        },
        "source_traceability_remediation_status": "source_traceability_remediated",
        "source_traceability_status": "ready",
        "traceability_status": "ready",
        "validity_dependency_status": "not_assessed_in_2413"
      },
      "before": {
        "as_of_semantics_status": null,
        "contract_ready": false,
        "feature_id": "equal_risk_baseline_weights",
        "mapping_status": "missing_source_traceability",
        "pit_gate_status": null,
        "source_feature_name": "equal_risk_baseline_weights",
        "source_traceability_status": null,
        "traceability_status": "mapped_with_caveats",
        "validity_dependency_status": null
      },
      "feature_id": "equal_risk_baseline_weights"
    },
    {
      "after": {
        "as_of_remediation_status": null,
        "as_of_semantics_status": null,
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "as_of_semantics_status",
          "source_traceability_status",
          "validity_dependency_status",
          "pit_gate_status"
        ],
        "feature_id": "target_vol_policy",
        "mapping_status": "blocked_unresolved",
        "mapping_status_reason": "source traceability explicitly classified in TRADING-2413 but remains blocked; validity dependency and PIT gate evidence also remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "target_vol_policy",
        "source_traceability_contract_metadata": {
          "as_of_semantics_status": null,
          "broker_action": "none",
          "contract_ready": false,
          "derived_from_prior_artifact": false,
          "fresh_market_data_required": false,
          "pit_gate_status": "blocked_pending_pit_evidence",
          "production_effect": "none",
          "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
          "source_feature_id": "target_vol_policy",
          "source_feature_name": "target_vol_policy",
          "source_snapshot_hash": null,
          "source_snapshot_reference": null,
          "source_traceability_contract_id": "growth_tilt_engine:target_vol_policy:source_traceability:v1",
          "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
          "traceability_blocking_reason": "missing governed config key: search_grids.vol_target_growth_tilt",
          "traceability_status": "not_ready",
          "upstream_artifact_id": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
          "upstream_artifact_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
          "upstream_config_key": "search_grids.vol_target_growth_tilt",
          "upstream_config_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
          "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
          "upstream_source_system": "governed_config",
          "validity_dependency_status": "not_assessed_in_2413"
        },
        "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
        "source_traceability_status": "not_ready",
        "traceability_status": "blocked",
        "validity_dependency_status": "not_assessed_in_2413"
      },
      "before": {
        "as_of_semantics_status": null,
        "contract_ready": false,
        "feature_id": "target_vol_policy",
        "mapping_status": "missing_source_traceability",
        "pit_gate_status": null,
        "source_feature_name": "target_vol_policy",
        "source_traceability_status": null,
        "traceability_status": "mapped_with_caveats",
        "validity_dependency_status": null
      },
      "feature_id": "target_vol_policy"
    },
    {
      "after": {
        "as_of_remediation_status": null,
        "as_of_semantics_status": null,
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "as_of_semantics_status",
          "source_traceability_status",
          "validity_dependency_status",
          "pit_gate_status"
        ],
        "feature_id": "trend_features",
        "mapping_status": "blocked_unresolved",
        "mapping_status_reason": "source traceability explicitly classified in TRADING-2413 but remains blocked; validity dependency and PIT gate evidence also remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "trend_features",
        "source_traceability_contract_metadata": {
          "as_of_semantics_status": null,
          "broker_action": "none",
          "contract_ready": false,
          "derived_from_prior_artifact": true,
          "fresh_market_data_required": false,
          "pit_gate_status": "blocked_pending_pit_evidence",
          "production_effect": "none",
          "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
          "source_feature_id": "trend_features",
          "source_feature_name": "trend_features",
          "source_snapshot_hash": null,
          "source_snapshot_reference": null,
          "source_traceability_contract_id": "growth_tilt_engine:trend_features:source_traceability:v1",
          "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
          "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
          "traceability_status": "not_ready",
          "upstream_artifact_id": "historical price trend / momentum windows",
          "upstream_artifact_path": null,
          "upstream_config_key": null,
          "upstream_config_path": null,
          "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
          "upstream_source_system": "derived_research_artifact",
          "validity_dependency_status": "not_assessed_in_2413"
        },
        "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
        "source_traceability_status": "not_ready",
        "traceability_status": "blocked",
        "validity_dependency_status": "not_assessed_in_2413"
      },
      "before": {
        "as_of_semantics_status": null,
        "contract_ready": false,
        "feature_id": "trend_features",
        "mapping_status": "missing_source_traceability",
        "pit_gate_status": null,
        "source_feature_name": "trend_features",
        "source_traceability_status": null,
        "traceability_status": "partial",
        "validity_dependency_status": null
      },
      "feature_id": "trend_features"
    },
    {
      "after": {
        "as_of_remediation_status": "as_of_semantics_remediated",
        "as_of_semantics_status": "ready",
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "source_traceability_status",
          "validity_dependency_status",
          "pit_gate_status"
        ],
        "feature_id": "volatility_inputs",
        "mapping_status": "blocked_unresolved",
        "mapping_status_reason": "source traceability explicitly classified in TRADING-2413 but remains blocked; validity dependency and PIT gate evidence also remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "volatility_inputs",
        "source_traceability_contract_metadata": {
          "as_of_semantics_status": "ready",
          "broker_action": "none",
          "contract_ready": false,
          "derived_from_prior_artifact": true,
          "fresh_market_data_required": false,
          "pit_gate_status": "blocked_pending_pit_evidence",
          "production_effect": "none",
          "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
          "source_feature_id": "volatility_inputs",
          "source_feature_name": "volatility_inputs",
          "source_snapshot_hash": null,
          "source_snapshot_reference": null,
          "source_traceability_contract_id": "growth_tilt_engine:volatility_inputs:source_traceability:v1",
          "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
          "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
          "traceability_status": "not_ready",
          "upstream_artifact_id": "rolling price-derived volatility features",
          "upstream_artifact_path": null,
          "upstream_config_key": null,
          "upstream_config_path": null,
          "upstream_report_registry_id": "growth_tilt_engine_as_of_semantics_remediation",
          "upstream_source_system": "derived_research_artifact",
          "validity_dependency_status": "not_assessed_in_2412"
        },
        "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
        "source_traceability_status": "not_ready",
        "traceability_status": "blocked",
        "validity_dependency_status": "not_assessed_in_2412"
      },
      "before": {
        "as_of_semantics_status": "ready",
        "contract_ready": false,
        "feature_id": "volatility_inputs",
        "mapping_status": "mapped_with_caveats",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "source_feature_name": "volatility_inputs",
        "source_traceability_status": "not_ready_missing_source_snapshot",
        "traceability_status": "missing",
        "validity_dependency_status": "not_assessed_in_2412"
      },
      "feature_id": "volatility_inputs"
    },
    {
      "after": {
        "as_of_remediation_status": "as_of_semantics_remediated",
        "as_of_semantics_status": "ready",
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "source_traceability_status",
          "validity_dependency_status",
          "pit_gate_status"
        ],
        "feature_id": "drawdown_features",
        "mapping_status": "blocked_unresolved",
        "mapping_status_reason": "source traceability explicitly classified in TRADING-2413 but remains blocked; validity dependency and PIT gate evidence also remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "drawdown_features",
        "source_traceability_contract_metadata": {
          "as_of_semantics_status": "ready",
          "broker_action": "none",
          "contract_ready": false,
          "derived_from_prior_artifact": true,
          "fresh_market_data_required": false,
          "pit_gate_status": "blocked_pending_pit_evidence",
          "production_effect": "none",
          "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
          "source_feature_id": "drawdown_features",
          "source_feature_name": "drawdown_features",
          "source_snapshot_hash": null,
          "source_snapshot_reference": null,
          "source_traceability_contract_id": "growth_tilt_engine:drawdown_features:source_traceability:v1",
          "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
          "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
          "traceability_status": "not_ready",
          "upstream_artifact_id": "historical drawdown windows",
          "upstream_artifact_path": null,
          "upstream_config_key": null,
          "upstream_config_path": null,
          "upstream_report_registry_id": "growth_tilt_engine_as_of_semantics_remediation",
          "upstream_source_system": "derived_research_artifact",
          "validity_dependency_status": "not_assessed_in_2412"
        },
        "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
        "source_traceability_status": "not_ready",
        "traceability_status": "blocked",
        "validity_dependency_status": "not_assessed_in_2412"
      },
      "before": {
        "as_of_semantics_status": "ready",
        "contract_ready": false,
        "feature_id": "drawdown_features",
        "mapping_status": "mapped_with_caveats",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "source_feature_name": "drawdown_features",
        "source_traceability_status": "not_ready_missing_source_snapshot",
        "traceability_status": "missing",
        "validity_dependency_status": "not_assessed_in_2412"
      },
      "feature_id": "drawdown_features"
    },
    {
      "after": {
        "as_of_remediation_status": null,
        "as_of_semantics_status": null,
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "as_of_semantics_status",
          "validity_dependency_status",
          "pit_gate_status"
        ],
        "feature_id": "risk_on_trend_filter_context",
        "mapping_status": "mapped_with_caveats",
        "mapping_status_reason": "source traceability remediated in TRADING-2413; validity dependency and PIT gate evidence remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "risk_on_trend_filter_context",
        "source_traceability_contract_metadata": {
          "as_of_semantics_status": null,
          "broker_action": "none",
          "contract_ready": false,
          "derived_from_prior_artifact": false,
          "fresh_market_data_required": false,
          "pit_gate_status": "blocked_pending_pit_evidence",
          "production_effect": "none",
          "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
          "source_feature_id": "risk_on_trend_filter_context",
          "source_feature_name": "risk_on_trend_filter_context",
          "source_snapshot_hash": "sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
          "source_snapshot_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule@sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
          "source_traceability_contract_id": "growth_tilt_engine:risk_on_trend_filter_context:source_traceability:v1",
          "source_traceability_remediation_status": "source_traceability_remediated",
          "traceability_blocking_reason": null,
          "traceability_status": "ready",
          "upstream_artifact_id": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
          "upstream_artifact_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
          "upstream_config_key": "research_policy.trend_filter_rule",
          "upstream_config_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
          "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
          "upstream_source_system": "governed_config",
          "validity_dependency_status": "not_assessed_in_2413"
        },
        "source_traceability_remediation_status": "source_traceability_remediated",
        "source_traceability_status": "ready",
        "traceability_status": "ready",
        "validity_dependency_status": "not_assessed_in_2413"
      },
      "before": {
        "as_of_semantics_status": null,
        "contract_ready": false,
        "feature_id": "risk_on_trend_filter_context",
        "mapping_status": "mapped_with_caveats",
        "pit_gate_status": null,
        "source_feature_name": "risk_on_trend_filter_context",
        "source_traceability_status": null,
        "traceability_status": "missing",
        "validity_dependency_status": null
      },
      "feature_id": "risk_on_trend_filter_context"
    },
    {
      "after": {
        "as_of_remediation_status": null,
        "as_of_semantics_status": null,
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "as_of_semantics_status",
          "source_traceability_status",
          "validity_dependency_status",
          "pit_gate_status"
        ],
        "feature_id": "growth_tilt_engine_signal_artifact",
        "mapping_status": "blocked_unresolved",
        "mapping_status_reason": "source traceability explicitly classified in TRADING-2413 but remains blocked; validity dependency and PIT gate evidence also remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "growth_tilt_engine_signal_artifact",
        "source_traceability_contract_metadata": {
          "as_of_semantics_status": null,
          "broker_action": "none",
          "contract_ready": false,
          "derived_from_prior_artifact": true,
          "fresh_market_data_required": false,
          "pit_gate_status": "blocked_pending_pit_evidence",
          "production_effect": "none",
          "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
          "source_feature_id": "growth_tilt_engine_signal_artifact",
          "source_feature_name": "growth_tilt_engine_signal_artifact",
          "source_snapshot_hash": null,
          "source_snapshot_reference": null,
          "source_traceability_contract_id": "growth_tilt_engine:growth_tilt_engine_signal_artifact:source_traceability:v1",
          "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
          "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
          "traceability_status": "not_ready",
          "upstream_artifact_id": "missing standalone growth_tilt_engine signal artifact",
          "upstream_artifact_path": null,
          "upstream_config_key": null,
          "upstream_config_path": null,
          "upstream_report_registry_id": "growth_tilt_engine_contract_gap_remediation_plan",
          "upstream_source_system": "missing_artifact",
          "validity_dependency_status": "not_assessed_in_2413"
        },
        "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
        "source_traceability_status": "not_ready",
        "traceability_status": "blocked",
        "validity_dependency_status": "not_assessed_in_2413"
      },
      "before": {
        "as_of_semantics_status": null,
        "contract_ready": false,
        "feature_id": "growth_tilt_engine_signal_artifact",
        "mapping_status": "blocked_unresolved",
        "pit_gate_status": null,
        "source_feature_name": "growth_tilt_engine_signal_artifact",
        "source_traceability_status": null,
        "traceability_status": "missing",
        "validity_dependency_status": null
      },
      "feature_id": "growth_tilt_engine_signal_artifact"
    }
  ],
  "schema_version": "growth_tilt_engine_source_traceability_before_after_remediation.v1"
}
```

## Source Traceability Contract Metadata

```json
{
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "fresh_market_data_required": false,
  "metadata_rows": [
    {
      "as_of_semantics_status": null,
      "broker_action": "none",
      "contract_ready": false,
      "derived_from_prior_artifact": false,
      "fresh_market_data_required": false,
      "pit_gate_status": "blocked_pending_pit_evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
      "source_feature_id": "equal_risk_baseline_weights",
      "source_feature_name": "equal_risk_baseline_weights",
      "source_snapshot_hash": "sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
      "source_snapshot_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk@sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
      "source_traceability_contract_id": "growth_tilt_engine:equal_risk_baseline_weights:source_traceability:v1",
      "source_traceability_remediation_status": "source_traceability_remediated",
      "traceability_blocking_reason": null,
      "traceability_status": "ready",
      "upstream_artifact_id": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
      "upstream_artifact_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
      "upstream_config_key": "research_policy.equal_risk",
      "upstream_config_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
      "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
      "upstream_source_system": "governed_config",
      "validity_dependency_status": "not_assessed_in_2413"
    },
    {
      "as_of_semantics_status": null,
      "broker_action": "none",
      "contract_ready": false,
      "derived_from_prior_artifact": false,
      "fresh_market_data_required": false,
      "pit_gate_status": "blocked_pending_pit_evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
      "source_feature_id": "target_vol_policy",
      "source_feature_name": "target_vol_policy",
      "source_snapshot_hash": null,
      "source_snapshot_reference": null,
      "source_traceability_contract_id": "growth_tilt_engine:target_vol_policy:source_traceability:v1",
      "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
      "traceability_blocking_reason": "missing governed config key: search_grids.vol_target_growth_tilt",
      "traceability_status": "not_ready",
      "upstream_artifact_id": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
      "upstream_artifact_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
      "upstream_config_key": "search_grids.vol_target_growth_tilt",
      "upstream_config_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
      "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
      "upstream_source_system": "governed_config",
      "validity_dependency_status": "not_assessed_in_2413"
    },
    {
      "as_of_semantics_status": null,
      "broker_action": "none",
      "contract_ready": false,
      "derived_from_prior_artifact": true,
      "fresh_market_data_required": false,
      "pit_gate_status": "blocked_pending_pit_evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
      "source_feature_id": "trend_features",
      "source_feature_name": "trend_features",
      "source_snapshot_hash": null,
      "source_snapshot_reference": null,
      "source_traceability_contract_id": "growth_tilt_engine:trend_features:source_traceability:v1",
      "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
      "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
      "traceability_status": "not_ready",
      "upstream_artifact_id": "historical price trend / momentum windows",
      "upstream_artifact_path": null,
      "upstream_config_key": null,
      "upstream_config_path": null,
      "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
      "upstream_source_system": "derived_research_artifact",
      "validity_dependency_status": "not_assessed_in_2413"
    },
    {
      "as_of_semantics_status": "ready",
      "broker_action": "none",
      "contract_ready": false,
      "derived_from_prior_artifact": true,
      "fresh_market_data_required": false,
      "pit_gate_status": "blocked_pending_pit_evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
      "source_feature_id": "volatility_inputs",
      "source_feature_name": "volatility_inputs",
      "source_snapshot_hash": null,
      "source_snapshot_reference": null,
      "source_traceability_contract_id": "growth_tilt_engine:volatility_inputs:source_traceability:v1",
      "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
      "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
      "traceability_status": "not_ready",
      "upstream_artifact_id": "rolling price-derived volatility features",
      "upstream_artifact_path": null,
      "upstream_config_key": null,
      "upstream_config_path": null,
      "upstream_report_registry_id": "growth_tilt_engine_as_of_semantics_remediation",
      "upstream_source_system": "derived_research_artifact",
      "validity_dependency_status": "not_assessed_in_2412"
    },
    {
      "as_of_semantics_status": "ready",
      "broker_action": "none",
      "contract_ready": false,
      "derived_from_prior_artifact": true,
      "fresh_market_data_required": false,
      "pit_gate_status": "blocked_pending_pit_evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
      "source_feature_id": "drawdown_features",
      "source_feature_name": "drawdown_features",
      "source_snapshot_hash": null,
      "source_snapshot_reference": null,
      "source_traceability_contract_id": "growth_tilt_engine:drawdown_features:source_traceability:v1",
      "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
      "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
      "traceability_status": "not_ready",
      "upstream_artifact_id": "historical drawdown windows",
      "upstream_artifact_path": null,
      "upstream_config_key": null,
      "upstream_config_path": null,
      "upstream_report_registry_id": "growth_tilt_engine_as_of_semantics_remediation",
      "upstream_source_system": "derived_research_artifact",
      "validity_dependency_status": "not_assessed_in_2412"
    },
    {
      "as_of_semantics_status": null,
      "broker_action": "none",
      "contract_ready": false,
      "derived_from_prior_artifact": false,
      "fresh_market_data_required": false,
      "pit_gate_status": "blocked_pending_pit_evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
      "source_feature_id": "risk_on_trend_filter_context",
      "source_feature_name": "risk_on_trend_filter_context",
      "source_snapshot_hash": "sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
      "source_snapshot_reference": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule@sha256:e65c4e3f325a2b381d2cd4da785e5857fab5fe8421455f16461d0ec1e7fb1e57",
      "source_traceability_contract_id": "growth_tilt_engine:risk_on_trend_filter_context:source_traceability:v1",
      "source_traceability_remediation_status": "source_traceability_remediated",
      "traceability_blocking_reason": null,
      "traceability_status": "ready",
      "upstream_artifact_id": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
      "upstream_artifact_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
      "upstream_config_key": "research_policy.trend_filter_rule",
      "upstream_config_path": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
      "upstream_report_registry_id": "growth_tilt_engine_source_feature_contract_mapping",
      "upstream_source_system": "governed_config",
      "validity_dependency_status": "not_assessed_in_2413"
    },
    {
      "as_of_semantics_status": null,
      "broker_action": "none",
      "contract_ready": false,
      "derived_from_prior_artifact": true,
      "fresh_market_data_required": false,
      "pit_gate_status": "blocked_pending_pit_evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1",
      "source_feature_id": "growth_tilt_engine_signal_artifact",
      "source_feature_name": "growth_tilt_engine_signal_artifact",
      "source_snapshot_hash": null,
      "source_snapshot_reference": null,
      "source_traceability_contract_id": "growth_tilt_engine:growth_tilt_engine_signal_artifact:source_traceability:v1",
      "source_traceability_remediation_status": "source_traceability_blocked_by_missing_upstream_artifact",
      "traceability_blocking_reason": "missing upstream artifact path or source snapshot reference",
      "traceability_status": "not_ready",
      "upstream_artifact_id": "missing standalone growth_tilt_engine signal artifact",
      "upstream_artifact_path": null,
      "upstream_config_key": null,
      "upstream_config_path": null,
      "upstream_report_registry_id": "growth_tilt_engine_contract_gap_remediation_plan",
      "upstream_source_system": "missing_artifact",
      "validity_dependency_status": "not_assessed_in_2413"
    }
  ],
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_source_traceability_contract_metadata.v1"
}
```

## Remaining Blockers

```json
{
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
  "production_effect": "none",
  "production_enabled": false,
  "recommended_next_task": "TRADING-2414_Growth_Tilt_Engine_Signal_Validity_Dependency_Remediation",
  "remaining_blocked_or_gap_count": 7,
  "remaining_blocking_reasons": [
    "derived_window_source_snapshots_not_completed_in_2413",
    "signal_validity_dependency_not_completed_in_2413",
    "pit_gate_evidence_not_completed_in_2413",
    "valid_until_window_blocker_not_remediated_in_2413"
  ],
  "remaining_source_traceability_gap_count": 5,
  "schema_version": "growth_tilt_engine_source_traceability_remaining_blocker_summary.v1",
  "source_traceability_gap_count": 7,
  "source_traceability_remediated_count": 2,
  "valid_until_window_blocker_downgraded": false,
  "valid_until_window_blocker_resolved": false
}
```

## Data Quality Boundary

- data_quality_gate_executed：`False`
- data_quality_gate_reason：`NOT_APPLICABLE_SOURCE_TRACEABILITY_REMEDIATION_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

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