# Dynamic strategy source feature traceability contract schema

- status：`DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`
- schema ready：`True`
- next route：`TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`

```json
{
  "broker_action": "none",
  "fields": {
    "as_of_handling": {
      "required": true,
      "type": "enum",
      "values": [
        "EXPLICIT_AS_OF",
        "DERIVED_FROM_SOURCE_CUTOFF",
        "APPROXIMATE",
        "UNKNOWN"
      ]
    },
    "feature_family": {
      "required": true,
      "type": "string"
    },
    "feature_id": {
      "required": true,
      "type": "string"
    },
    "forward_window_used": {
      "required": true,
      "type": "boolean"
    },
    "generated_at_handling": {
      "required": true,
      "type": "enum",
      "values": [
        "EXPLICIT_GENERATED_AT",
        "DERIVED_FROM_PIPELINE_RUN",
        "APPROXIMATE",
        "UNKNOWN"
      ]
    },
    "lookback_window": {
      "required": false,
      "type": "integer_or_null"
    },
    "pit_confidence": {
      "required": true,
      "type": "enum",
      "values": [
        "HIGH",
        "MEDIUM",
        "LOW",
        "UNKNOWN"
      ]
    },
    "pit_status": {
      "required": true,
      "type": "enum",
      "values": [
        "TRUE_PIT",
        "APPROXIMATE_PIT",
        "NOT_PIT_SAFE",
        "UNKNOWN",
        "NOT_APPLICABLE"
      ]
    },
    "risk_flags": {
      "required": false,
      "type": "list",
      "values": [
        "LOOKAHEAD_RISK",
        "REVISION_RISK",
        "BACKFILL_RISK",
        "STALE_DATA_RISK",
        "MISSING_DATA_RISK",
        "REGIME_CONFIRMATION_RISK",
        "VALID_UNTIL_UNGROUNDED",
        "THRESHOLD_UNCALIBRATED"
      ]
    },
    "severity": {
      "required": true,
      "type": "enum",
      "values": [
        "BLOCKING",
        "MATERIAL",
        "MINOR",
        "INFO"
      ]
    },
    "source_config": {
      "required": true,
      "type": "string"
    },
    "source_data": {
      "required": true,
      "type": "string_or_list"
    }
  },
  "invariants": [
    "every_signal_feature_has_feature_id",
    "forward_window_used=false_for_TRUE_PIT_features",
    "pit_status_UNKNOWN_requires_LOW_or_UNKNOWN_confidence",
    "severity_BLOCKING_requires_risk_flag_or_explicit_reason",
    "NOT_PIT_SAFE_cannot_have_HIGH_confidence"
  ],
  "optional_fields": [
    "lookback_window",
    "risk_flags",
    "explicit_reason"
  ],
  "production_effect": "none",
  "required_fields": [
    "feature_id",
    "feature_family",
    "source_config",
    "source_data",
    "as_of_handling",
    "generated_at_handling",
    "forward_window_used",
    "pit_status",
    "pit_confidence",
    "severity"
  ],
  "schema_name": "source_feature_traceability_contract",
  "schema_version": "source_feature_traceability_contract.v1",
  "validation_error_codes": [
    "REQUIRED_FIELD_MISSING",
    "INVALID_ENUM_VALUE",
    "INVALID_PIT_CONFIDENCE_COMBINATION",
    "FORWARD_WINDOW_CONFLICTS_WITH_TRUE_PIT",
    "BLOCKING_SEVERITY_REQUIRES_REASON"
  ]
}
```