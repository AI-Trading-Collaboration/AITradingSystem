# Dynamic strategy signal validity contract schema

- status：`DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`
- schema ready：`True`
- next route：`TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`

```json
{
  "broker_action": "none",
  "fields": {
    "carry_forward_rule": {
      "required": true,
      "type": "enum",
      "values": [
        "FORBIDDEN",
        "ALLOWED_WITH_EXPLICIT_RULE",
        "ALLOWED_WITH_OWNER_APPROVAL",
        "UNKNOWN"
      ]
    },
    "expiry_rule": {
      "required": true,
      "type": "enum_or_string"
    },
    "horizon_days": {
      "required": true,
      "type": "integer"
    },
    "near_expiry_rule": {
      "required": true,
      "type": "enum",
      "values": [
        "BLOCK",
        "DECAY",
        "REQUIRE_REFRESH",
        "ALLOW_WITH_CAVEAT",
        "UNKNOWN"
      ]
    },
    "signal_id": {
      "required": true,
      "type": "string"
    },
    "signal_to_execution_lag_rule": {
      "required": true,
      "type": "enum_or_string"
    },
    "signal_version": {
      "required": true,
      "type": "string"
    },
    "stale_after": {
      "required": true,
      "type": "date_or_datetime"
    },
    "valid_from": {
      "required": true,
      "type": "date_or_datetime"
    },
    "valid_until": {
      "required": true,
      "type": "date_or_datetime"
    }
  },
  "invariants": [
    "valid_until > valid_from",
    "stale_after <= valid_until",
    "horizon_days > 0",
    "expired_signal_cannot_trigger_new_trade",
    "carry_forward_requires_explicit_rule",
    "missing_valid_until_blocks_candidate_search_for_dependent_strategy",
    "signal_to_execution_lag_rule_must_be_present"
  ],
  "production_effect": "none",
  "required_fields": [
    "signal_id",
    "signal_version",
    "valid_from",
    "valid_until",
    "stale_after",
    "horizon_days",
    "expiry_rule",
    "carry_forward_rule",
    "near_expiry_rule",
    "signal_to_execution_lag_rule"
  ],
  "schema_name": "signal_validity_contract",
  "schema_version": "signal_validity_contract.v1",
  "validation_error_codes": [
    "REQUIRED_FIELD_MISSING",
    "INVALID_ENUM_VALUE",
    "INVALID_DATE_ORDER",
    "INVALID_HORIZON",
    "CARRY_FORWARD_RULE_UNKNOWN",
    "VALID_UNTIL_MISSING_OR_INVALID"
  ]
}
```