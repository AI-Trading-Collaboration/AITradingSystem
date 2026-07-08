# Growth tilt engine upstream artifact closure evidence

## 摘要

本文件记录 source traceability closure 对应的 upstream artifact / registry
evidence。`growth_tilt_engine_signal_artifact` 仍未闭合，因为缺少独立
signal artifact metadata。

```json
{
  "auto_mark_contract_ready": false,
  "auto_mark_pit_gate_ready": false,
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "evidence_rows": [
    {
      "artifact_checksum_available": false,
      "artifact_row_count_available": false,
      "broker_action": "none",
      "contract_ready_after_2417": false,
      "evidence_limitations": [
        "source_snapshot_hash_not_available_in_prior_artifacts",
        "does_not_mark_contract_or_PIT_gate_ready"
      ],
      "feature_id": "volatility_inputs",
      "pit_gate_ready_after_2417": false,
      "production_effect": "none",
      "source_artifact_after_2417": "rolling price-derived volatility features",
      "source_snapshot_hash_after_2417": null,
      "source_snapshot_reference_after_2417": "outputs/research_strategies/growth_tilt_engine_as_of_semantics_remediation/updated_source_feature_mapping.json#mapping_rows[feature_id=volatility_inputs]",
      "upstream_artifact_available_after_2417": true,
      "upstream_artifact_closure_status": "PRE_RECHECK_UPSTREAM_EVIDENCE_AVAILABLE",
      "upstream_artifact_key_after_2417": "mapping_rows[feature_id=volatility_inputs]",
      "upstream_artifact_path_after_2417": "outputs/research_strategies/growth_tilt_engine_as_of_semantics_remediation/updated_source_feature_mapping.json",
      "upstream_report_registry_id_after_2417": "growth_tilt_engine_as_of_semantics_remediation"
    },
    {
      "artifact_checksum_available": false,
      "artifact_row_count_available": false,
      "broker_action": "none",
      "contract_ready_after_2417": false,
      "evidence_limitations": [
        "source_snapshot_hash_not_available_in_prior_artifacts",
        "as_of_contract_metadata_still_requires_PIT_recheck",
        "does_not_mark_contract_or_PIT_gate_ready"
      ],
      "feature_id": "trend_features",
      "pit_gate_ready_after_2417": false,
      "production_effect": "none",
      "source_artifact_after_2417": "historical price trend / momentum windows",
      "source_snapshot_hash_after_2417": null,
      "source_snapshot_reference_after_2417": "outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/source_feature_contract_mapping.json#mapping_rows[feature_id=trend_features]",
      "upstream_artifact_available_after_2417": true,
      "upstream_artifact_closure_status": "PRE_RECHECK_UPSTREAM_EVIDENCE_AVAILABLE",
      "upstream_artifact_key_after_2417": "mapping_rows[feature_id=trend_features]",
      "upstream_artifact_path_after_2417": "outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/source_feature_contract_mapping.json",
      "upstream_report_registry_id_after_2417": "growth_tilt_engine_source_feature_contract_mapping"
    },
    {
      "artifact_checksum_available": false,
      "artifact_row_count_available": false,
      "broker_action": "none",
      "contract_ready_after_2417": false,
      "evidence_limitations": [
        "source_snapshot_hash_not_available_in_prior_artifacts",
        "does_not_mark_contract_or_PIT_gate_ready"
      ],
      "feature_id": "drawdown_features",
      "pit_gate_ready_after_2417": false,
      "production_effect": "none",
      "source_artifact_after_2417": "historical drawdown windows",
      "source_snapshot_hash_after_2417": null,
      "source_snapshot_reference_after_2417": "outputs/research_strategies/growth_tilt_engine_as_of_semantics_remediation/updated_source_feature_mapping.json#mapping_rows[feature_id=drawdown_features]",
      "upstream_artifact_available_after_2417": true,
      "upstream_artifact_closure_status": "PRE_RECHECK_UPSTREAM_EVIDENCE_AVAILABLE",
      "upstream_artifact_key_after_2417": "mapping_rows[feature_id=drawdown_features]",
      "upstream_artifact_path_after_2417": "outputs/research_strategies/growth_tilt_engine_as_of_semantics_remediation/updated_source_feature_mapping.json",
      "upstream_report_registry_id_after_2417": "growth_tilt_engine_as_of_semantics_remediation"
    },
    {
      "artifact_checksum_available": false,
      "artifact_row_count_available": false,
      "broker_action": "none",
      "contract_ready_after_2417": false,
      "evidence_limitations": [
        "source_snapshot_hash_not_available_in_prior_artifacts",
        "does_not_mark_contract_or_PIT_gate_ready"
      ],
      "feature_id": "target_vol_policy",
      "pit_gate_ready_after_2417": false,
      "production_effect": "none",
      "source_artifact_after_2417": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
      "source_snapshot_hash_after_2417": null,
      "source_snapshot_reference_after_2417": "config/research/equal_risk_growth_tilt_candidate_registry.yaml#mapping_rows[feature_id=target_vol_policy]",
      "upstream_artifact_available_after_2417": true,
      "upstream_artifact_closure_status": "PRE_RECHECK_UPSTREAM_EVIDENCE_AVAILABLE",
      "upstream_artifact_key_after_2417": "search_grids.vol_target_growth_tilt",
      "upstream_artifact_path_after_2417": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
      "upstream_report_registry_id_after_2417": "growth_tilt_engine_source_feature_contract_mapping"
    },
    {
      "artifact_checksum_available": false,
      "artifact_row_count_available": false,
      "broker_action": "none",
      "contract_ready_after_2417": false,
      "evidence_limitations": [
        "standalone_signal_artifact_metadata_missing",
        "source_config_missing",
        "source_snapshot_reference_missing"
      ],
      "feature_id": "growth_tilt_engine_signal_artifact",
      "pit_gate_ready_after_2417": false,
      "production_effect": "none",
      "source_artifact_after_2417": null,
      "source_snapshot_hash_after_2417": null,
      "source_snapshot_reference_after_2417": null,
      "upstream_artifact_available_after_2417": false,
      "upstream_artifact_closure_status": "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT",
      "upstream_artifact_key_after_2417": null,
      "upstream_artifact_path_after_2417": null,
      "upstream_report_registry_id_after_2417": "missing_standalone_growth_tilt_engine_signal_artifact"
    }
  ],
  "evidence_scope": "pre_recheck_upstream_artifact_evidence",
  "pit_gate_recheck_required": true,
  "pre_recheck_evidence_ready_count": 4,
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_upstream_artifact_closure_evidence.v1",
  "still_blocked_count": 1,
  "upstream_artifact_gap_count_from_2416": 1
}
```
