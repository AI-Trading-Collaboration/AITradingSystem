# Growth tilt engine contract mapping validation

- status：`GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED`
- validation ready：`True`
- 下一路线：`TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan`

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