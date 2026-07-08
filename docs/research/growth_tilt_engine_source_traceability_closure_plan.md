# Growth tilt engine source traceability closure plan

- status：`GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY`
- source traceability blocker count：`5`
- next route：`TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure`

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