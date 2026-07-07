# Dynamic strategy signal as-of contract schema

- status：`DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`
- schema ready：`True`
- next route：`TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`

```json
{
  "broker_action": "none",
  "fields": {
    "as_of_date": {
      "required": true,
      "type": "date"
    },
    "confidence": {
      "required": false,
      "type": "number_or_enum"
    },
    "generated_at": {
      "required": true,
      "type": "datetime"
    },
    "signal_horizon_days": {
      "required": true,
      "type": "integer"
    },
    "signal_id": {
      "required": true,
      "type": "string"
    },
    "signal_strength": {
      "required": false,
      "type": "number"
    },
    "signal_value": {
      "required": true,
      "type": "number_or_object"
    },
    "signal_version": {
      "required": true,
      "type": "string"
    },
    "source_artifact_ids": {
      "required": false,
      "type": "list[string]"
    },
    "source_data_cutoff": {
      "required": true,
      "type": "date_or_datetime"
    },
    "source_feature_ids": {
      "required": true,
      "type": "list[string]"
    },
    "uncertainty_reason": {
      "required": false,
      "type": "string_or_list"
    }
  },
  "invariants": [
    "generated_at >= source_data_cutoff",
    "as_of_date <= generated_at_or_research_as_of",
    "source_feature_ids_non_empty",
    "signal_horizon_days > 0",
    "no_forward_window_dependency_unless_explicitly_marked_not_pit_safe"
  ],
  "optional_fields": [
    "source_artifact_ids",
    "signal_strength",
    "confidence",
    "uncertainty_reason"
  ],
  "production_effect": "none",
  "required_fields": [
    "signal_id",
    "signal_version",
    "as_of_date",
    "generated_at",
    "source_data_cutoff",
    "source_feature_ids",
    "signal_horizon_days",
    "signal_value"
  ],
  "schema_name": "signal_as_of_contract",
  "schema_version": "signal_as_of_contract.v1",
  "validation_error_codes": [
    "REQUIRED_FIELD_MISSING",
    "INVALID_DATE_ORDER",
    "INVALID_HORIZON",
    "FORWARD_WINDOW_DEPENDENCY_NOT_MARKED_NOT_PIT_SAFE"
  ]
}
```