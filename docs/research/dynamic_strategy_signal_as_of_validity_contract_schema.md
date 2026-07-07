# Dynamic strategy signal as-of and validity contract schema

## Executive summary

- status：`DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`
- source tasks：`TRADING-2405, TRADING-2406, TRADING-2407, TRADING-2408`
- blocking gaps：`growth_tilt_engine, valid_until_window`
- signal as-of schema ready：`True`
- source feature traceability schema ready：`True`
- signal validity schema ready：`True`
- schema validation helpers ready：`True`
- contract snapshot ready：`True`
- PIT gate integration plan ready：`True`
- next route：`TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`

2409 实现 reusable contract schema 与基础 validator。它支持后续 `growth_tilt_engine` source feature mapping、`valid_until_window` validity mapping、as-of replay validation 和 owner downgrade review；本任务不清除 blocker、不恢复 candidate search、不批准 observation / paper-shadow / execution。

## Source findings from TRADING-2408

- 2408 source validation errors：`[]`
- data quality gate executed：`False`
- data quality gate reason：`NOT_APPLICABLE_SCHEMA_VALIDATOR_AND_PRIOR_VALIDATED_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## Signal as-of contract schema

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

## Source feature traceability contract schema

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

## Signal validity contract schema

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

## Schema validation helper behavior

```json
{
  "result_fields": [
    "valid",
    "schema_name",
    "error_count",
    "warning_count",
    "errors",
    "warnings"
  ],
  "validator_self_tests": {
    "signal_as_of_contract_valid_example": {
      "error_count": 0,
      "errors": [],
      "schema_name": "signal_as_of_contract",
      "valid": true,
      "warning_count": 0,
      "warnings": []
    },
    "signal_validity_contract_valid_example": {
      "error_count": 0,
      "errors": [],
      "schema_name": "signal_validity_contract",
      "valid": true,
      "warning_count": 0,
      "warnings": []
    },
    "source_feature_traceability_contract_valid_example": {
      "error_count": 0,
      "errors": [],
      "schema_name": "source_feature_traceability_contract",
      "valid": true,
      "warning_count": 0,
      "warnings": []
    }
  }
}
```

## Contract snapshot

```json
{
  "broker_action": "none",
  "contract_adoption_checklist": [
    "map source features to source_feature_traceability_contract",
    "map strategy signal outputs to signal_as_of_contract",
    "map valid_from / valid_until / stale_after fields to signal_validity_contract",
    "run as-of replay validation before blocker downgrade review",
    "regenerate PIT gate before candidate search reconsideration",
    "record owner review before any downgrade from BLOCKING"
  ],
  "contract_snapshot_ready": true,
  "production_effect": "none",
  "schema_validation_helpers_ready": true,
  "schema_version": "signal_contract_schema_snapshot.v1",
  "signal_as_of_contract": {
    "invariant_count": 5,
    "required_field_count": 8,
    "schema_name": "signal_as_of_contract",
    "schema_ready": true,
    "schema_version": "signal_as_of_contract.v1",
    "validation_error_codes": [
      "REQUIRED_FIELD_MISSING",
      "INVALID_DATE_ORDER",
      "INVALID_HORIZON",
      "FORWARD_WINDOW_DEPENDENCY_NOT_MARKED_NOT_PIT_SAFE"
    ]
  },
  "signal_validity_contract": {
    "invariant_count": 7,
    "required_field_count": 10,
    "schema_name": "signal_validity_contract",
    "schema_ready": true,
    "schema_version": "signal_validity_contract.v1",
    "validation_error_codes": [
      "REQUIRED_FIELD_MISSING",
      "INVALID_ENUM_VALUE",
      "INVALID_DATE_ORDER",
      "INVALID_HORIZON",
      "CARRY_FORWARD_RULE_UNKNOWN",
      "VALID_UNTIL_MISSING_OR_INVALID"
    ]
  },
  "source_feature_traceability_contract": {
    "invariant_count": 5,
    "required_field_count": 10,
    "schema_name": "source_feature_traceability_contract",
    "schema_ready": true,
    "schema_version": "source_feature_traceability_contract.v1",
    "validation_error_codes": [
      "REQUIRED_FIELD_MISSING",
      "INVALID_ENUM_VALUE",
      "INVALID_PIT_CONFIDENCE_COMBINATION",
      "FORWARD_WINDOW_CONFLICTS_WITH_TRUE_PIT",
      "BLOCKING_SEVERITY_REQUIRES_REASON"
    ]
  },
  "validator_self_test_results": {
    "signal_as_of_contract": {
      "error_count": 0,
      "errors": [],
      "schema_name": "signal_as_of_contract",
      "valid": true,
      "warning_count": 0,
      "warnings": []
    },
    "signal_validity_contract": {
      "error_count": 0,
      "errors": [],
      "schema_name": "signal_validity_contract",
      "valid": true,
      "warning_count": 0,
      "warnings": []
    },
    "source_feature_traceability_contract": {
      "error_count": 0,
      "errors": [],
      "schema_name": "source_feature_traceability_contract",
      "valid": true,
      "warning_count": 0,
      "warnings": []
    }
  }
}
```

## PIT gate integration plan

```json
{
  "broker_action": "none",
  "current_gate_change_in_2409": "none",
  "current_gate_result": {
    "blocking_gaps": [
      "growth_tilt_engine",
      "valid_until_window"
    ],
    "candidate_search_allowed": false,
    "paper_shadow_allowed": false,
    "production_allowed": false,
    "research_only_observation_allowed": false
  },
  "current_pit_gate_source": "config/research/dynamic_strategy_pit_input_registry.yaml",
  "future_integration": [
    "source feature contracts feed PIT matrix rows",
    "signal as-of contracts feed signal PIT status",
    "signal validity contracts feed valid_until_window status",
    "replay validation feeds blocker downgrade evidence"
  ],
  "production_effect": "none",
  "reconsider_candidate_search_only_after": [
    "growth_tilt_engine contract mapping completed",
    "valid_until_window contract mapping completed",
    "as-of replay validation dry run completed",
    "PIT gate regenerated",
    "owner review records blocker downgrade approval"
  ],
  "schema_version": "signal_contract_pit_gate_integration_plan.v1"
}
```

## Explicit non-approval list

```json
[
  "clear_growth_tilt_engine_blocking_gap",
  "clear_valid_until_window_blocking_gap",
  "downgrade_any_blocking_gap",
  "mark_any_blocker_true_pit",
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

- next task：`TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`
- reason：The shared schema is now ready; growth_tilt_engine source features must next be mapped to the source feature traceability and signal as-of contracts before replay validation or downgrade review.

## Safety boundary

- growth_tilt_engine_blocking_gap_resolved：`False`
- valid_until_window_blocking_gap_resolved：`False`
- any_blocker_severity_downgraded：`False`
- candidate_search_allowed：`False`
- research_only_observation_allowed：`False`
- paper_shadow_allowed：`False`
- production_allowed：`False`
- broker_action：`none`