# Growth Tilt Baseline Contract Adapters Readiness

M1D2 只把现有 baseline 事实物化为 versioned adapters。任何未解析 hard veto、requested/applied trace、native scalar、recovery PIT lineage 或 threshold 都保持 BLOCKED；没有新增 baseline 或 candidate 决策行为。

```json
{
  "adapter_contract_blocked_count": 4,
  "adapter_contract_ready_count": 0,
  "adapter_implementation_count": 4,
  "approved_candidate_count": 0,
  "as_of": "2026-07-10",
  "blocker_codes": [
    "APPLIED_TARGET_STATE_MISSING",
    "NATIVE_SCALAR_FIELD_MISSING:applied_target_scalar_field",
    "NATIVE_SCALAR_FIELD_MISSING:current_scalar_field",
    "NATIVE_SCALAR_FIELD_MISSING:maximum_value",
    "NATIVE_SCALAR_FIELD_MISSING:minimum_increment",
    "NATIVE_SCALAR_FIELD_MISSING:minimum_value",
    "NATIVE_SCALAR_FIELD_MISSING:native_scalar_id",
    "NATIVE_SCALAR_FIELD_MISSING:owner_semantics_status",
    "NATIVE_SCALAR_FIELD_MISSING:pit_lineage_ref",
    "NATIVE_SCALAR_FIELD_MISSING:requested_target_scalar_field",
    "NATIVE_SCALAR_FIELD_MISSING:unit",
    "NATIVE_SCALAR_INCREMENT_INVALID",
    "NATIVE_SCALAR_OWNER_SEMANTICS_NOT_APPROVED",
    "NATIVE_SCALAR_RANGE_NOT_FINITE",
    "RECOVERY_PIT_LINEAGE_INVALID_OR_MISSING",
    "RECOVERY_THRESHOLD_NOT_APPROVED",
    "REQUESTED_TARGET_STATE_MISSING",
    "UNRESOLVED_HARD_VETO:event_risk_veto:BLOCKED_NO_PIT_CONTRACT",
    "UNRESOLVED_HARD_VETO:risk_off_veto:BLOCKED_AMBIGUOUS_GROWTH_ALLOWED_ALIAS",
    "UNRESOLVED_HARD_VETO:trend_break_veto:BLOCKED_NO_CALLABLE_PRODUCER"
  ],
  "m2_eligible_candidate_count": 0,
  "next_route": "TRADING-2438M1E_GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT",
  "replacement_a_ready_for_m1e_approval": false,
  "status": "GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_READY_WITH_BLOCKERS",
  "strict_validation_error_count": 0
}
```

## Hard-veto aggregate adapter

```json
{
  "aggregate_materializable": false,
  "blocker_codes": [
    "UNRESOLVED_HARD_VETO:event_risk_veto:BLOCKED_NO_PIT_CONTRACT",
    "UNRESOLVED_HARD_VETO:risk_off_veto:BLOCKED_AMBIGUOUS_GROWTH_ALLOWED_ALIAS",
    "UNRESOLVED_HARD_VETO:trend_break_veto:BLOCKED_NO_CALLABLE_PRODUCER"
  ],
  "candidate_component_removal_allowed": false,
  "candidate_priority_change_allowed": false,
  "components": [
    {
      "blocker_codes": [
        "UNRESOLVED_HARD_VETO:risk_off_veto:BLOCKED_AMBIGUOUS_GROWTH_ALLOWED_ALIAS"
      ],
      "contract_ready": false,
      "missing_policy": "BLOCKED_NOT_FALSE",
      "output_path": "signal_state.risk_off_veto",
      "pit_lineage_ref": null,
      "priority": "BEFORE_CANDIDATE_OVERLAY",
      "producer_callable": true,
      "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
      "resolution_status": "BLOCKED_AMBIGUOUS_GROWTH_ALLOWED_ALIAS",
      "veto_id": "risk_off_veto"
    },
    {
      "blocker_codes": [],
      "contract_ready": true,
      "missing_policy": "BLOCKED_NOT_FALSE",
      "output_path": "signal_state.volatility_veto",
      "pit_lineage_ref": "channel_specific_first_layer_v3_final_matrix.v1",
      "priority": "BEFORE_CANDIDATE_OVERLAY",
      "producer_callable": true,
      "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
      "resolution_status": "RESOLVED_CALLABLE",
      "veto_id": "volatility_veto"
    },
    {
      "blocker_codes": [
        "UNRESOLVED_HARD_VETO:event_risk_veto:BLOCKED_NO_PIT_CONTRACT"
      ],
      "contract_ready": false,
      "missing_policy": "BLOCKED_NOT_FALSE",
      "output_path": "signal_state.event_risk_veto",
      "pit_lineage_ref": null,
      "priority": "BEFORE_CANDIDATE_OVERLAY",
      "producer_callable": false,
      "producer_entrypoint": null,
      "resolution_status": "BLOCKED_NO_PIT_CONTRACT",
      "veto_id": "event_risk_veto"
    },
    {
      "blocker_codes": [
        "UNRESOLVED_HARD_VETO:trend_break_veto:BLOCKED_NO_CALLABLE_PRODUCER"
      ],
      "contract_ready": false,
      "missing_policy": "BLOCKED_NOT_FALSE",
      "output_path": "signal_state.trend_break_veto",
      "pit_lineage_ref": null,
      "priority": "BEFORE_CANDIDATE_OVERLAY",
      "producer_callable": false,
      "producer_entrypoint": null,
      "resolution_status": "BLOCKED_NO_CALLABLE_PRODUCER",
      "veto_id": "trend_break_veto"
    },
    {
      "blocker_codes": [],
      "contract_ready": true,
      "missing_policy": "BLOCKED_NOT_FALSE",
      "output_path": "signal_state.tqqq_veto",
      "pit_lineage_ref": "base_overlay_veto_policy_schema.v1",
      "priority": "BEFORE_CANDIDATE_OVERLAY",
      "producer_callable": true,
      "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
      "resolution_status": "RESOLVED_CALLABLE",
      "veto_id": "tqqq_veto"
    }
  ],
  "missing_component_policy": "BLOCKED_NOT_FALSE",
  "raw_indicator_inputs_allowed": false,
  "required_component_ids": [
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
    "tqqq_veto"
  ],
  "resolved_component_ids": [
    "volatility_veto",
    "tqqq_veto"
  ],
  "schema_version": "growth_tilt_hard_veto_aggregate_adapter.v1",
  "status": "BLOCKED_UNRESOLVED_HARD_VETO_AGGREGATE",
  "unresolved_component_ids": [
    "risk_off_veto",
    "event_risk_veto",
    "trend_break_veto"
  ]
}
```

## Regime transition trace adapter

```json
{
  "blocked_record_count": 5,
  "blocker_codes": [
    "APPLIED_TARGET_STATE_MISSING",
    "REQUESTED_TARGET_STATE_MISSING"
  ],
  "candidate_may_supersede_baseline_defensive_request": false,
  "ordered_priority": [
    "INVALID_PIT_OR_DATA_CONTRACT_BLOCKED",
    "HARD_VETO_OR_EMERGENCY_RISK_REQUEST",
    "BASELINE_MANDATORY_DEFENSIVE_TRANSITION",
    "BASELINE_ORDINARY_TRANSITION_REQUEST",
    "APPROVED_RECOVERY_OVERLAY_REQUEST",
    "EXPOSURE_OR_RISK_CAP_CLAMP"
  ],
  "ready_record_count": 0,
  "record_count": 5,
  "records": [
    {
      "applied_at": null,
      "applied_target_state": null,
      "available_at": "2023-02-23",
      "blocker_codes": [
        "APPLIED_TARGET_STATE_MISSING",
        "REQUESTED_TARGET_STATE_MISSING"
      ],
      "current_state": "neutral",
      "known_at": "2023-02-23",
      "request_created_at": null,
      "requested_target_state": null,
      "source_date": "2023-02-23",
      "status": "BLOCKED",
      "trace_index": 0
    },
    {
      "applied_at": null,
      "applied_target_state": null,
      "available_at": "2023-02-24",
      "blocker_codes": [
        "APPLIED_TARGET_STATE_MISSING",
        "REQUESTED_TARGET_STATE_MISSING"
      ],
      "current_state": "defensive",
      "known_at": "2023-02-24",
      "request_created_at": null,
      "requested_target_state": null,
      "source_date": "2023-02-24",
      "status": "BLOCKED",
      "trace_index": 1
    },
    {
      "applied_at": null,
      "applied_target_state": null,
      "available_at": "2023-02-27",
      "blocker_codes": [
        "APPLIED_TARGET_STATE_MISSING",
        "REQUESTED_TARGET_STATE_MISSING"
      ],
      "current_state": "neutral",
      "known_at": "2023-02-27",
      "request_created_at": null,
      "requested_target_state": null,
      "source_date": "2023-02-27",
      "status": "BLOCKED",
      "trace_index": 2
    },
    {
      "applied_at": null,
      "applied_target_state": null,
      "available_at": "2023-09-06",
      "blocker_codes": [
        "APPLIED_TARGET_STATE_MISSING",
        "REQUESTED_TARGET_STATE_MISSING"
      ],
      "current_state": "constructive",
      "known_at": "2023-09-06",
      "request_created_at": null,
      "requested_target_state": null,
      "source_date": "2023-09-06",
      "status": "BLOCKED",
      "trace_index": 3
    },
    {
      "applied_at": null,
      "applied_target_state": null,
      "available_at": "2023-09-07",
      "blocker_codes": [
        "APPLIED_TARGET_STATE_MISSING",
        "REQUESTED_TARGET_STATE_MISSING"
      ],
      "current_state": "defensive",
      "known_at": "2023-09-07",
      "request_created_at": null,
      "requested_target_state": null,
      "source_date": "2023-09-07",
      "status": "BLOCKED",
      "trace_index": 4
    }
  ],
  "same_step_application_allowed": false,
  "schema_version": "growth_tilt_regime_transition_trace_adapter.v1",
  "status": "BLOCKED",
  "used_adjacent_row_inference": false
}
```

## Native exposure scalar adapter

```json
{
  "binding": {
    "applied_target_scalar_field": null,
    "current_scalar_field": null,
    "maximum_value": null,
    "minimum_increment": null,
    "minimum_value": null,
    "native_scalar_id": null,
    "owner_semantics_status": null,
    "pit_lineage_ref": null,
    "requested_target_scalar_field": null,
    "unit": null
  },
  "blocker_codes": [
    "NATIVE_SCALAR_FIELD_MISSING:applied_target_scalar_field",
    "NATIVE_SCALAR_FIELD_MISSING:current_scalar_field",
    "NATIVE_SCALAR_FIELD_MISSING:maximum_value",
    "NATIVE_SCALAR_FIELD_MISSING:minimum_increment",
    "NATIVE_SCALAR_FIELD_MISSING:minimum_value",
    "NATIVE_SCALAR_FIELD_MISSING:native_scalar_id",
    "NATIVE_SCALAR_FIELD_MISSING:owner_semantics_status",
    "NATIVE_SCALAR_FIELD_MISSING:pit_lineage_ref",
    "NATIVE_SCALAR_FIELD_MISSING:requested_target_scalar_field",
    "NATIVE_SCALAR_FIELD_MISSING:unit",
    "NATIVE_SCALAR_INCREMENT_INVALID",
    "NATIVE_SCALAR_OWNER_SEMANTICS_NOT_APPROVED",
    "NATIVE_SCALAR_RANGE_NOT_FINITE"
  ],
  "candidate_delta_materialized": false,
  "instrument_name_multiplier_inference_allowed": false,
  "qqq_equivalent_candidate_delta_allowed": false,
  "scalar_binding_ready": false,
  "schema_version": "growth_tilt_native_exposure_scalar_adapter.v1",
  "status": "BLOCKED_NO_GOVERNED_NATIVE_SCALAR",
  "tqqq_increase_allowed": false
}
```

## Recovery permission adapter

```json
{
  "baseline_recovery_persistence_created": false,
  "baseline_transition_emitted": false,
  "blocker_codes": [
    "RECOVERY_PIT_LINEAGE_INVALID_OR_MISSING",
    "RECOVERY_THRESHOLD_NOT_APPROVED"
  ],
  "default_threshold_allowed": false,
  "existing_pilot_threshold_reused": false,
  "missing_pit_lineage_fields": [
    "as_of",
    "available_at",
    "known_at",
    "source_data_cutoff"
  ],
  "output_path": "outputs/research_trends/channel_specific_v3/channel_composer_v3_predictions.csv:re_risk_allowed_probability",
  "prediction_header_fields": [
    "date",
    "do_not_de_risk_probability",
    "re_risk_allowed_probability",
    "growth_allowed",
    "add_risk_allowed",
    "tqqq_allowed",
    "veto_reasons",
    "confidence",
    "validity_days"
  ],
  "probability_interpretation_allowed": false,
  "producer_callable": true,
  "required_pit_lineage_fields": [
    "as_of",
    "known_at",
    "available_at",
    "source_data_cutoff"
  ],
  "schema_version": "growth_tilt_recovery_permission_adapter.v1",
  "semantic_type": "UNSCALED_SCORE",
  "signal_id": "re_risk_allowed_probability",
  "status": "BLOCKED",
  "threshold_status": "BLOCKED_UNCALIBRATED_SCORE",
  "trigger_materialized": false
}
```

## 结论

adapter code 已存在且 fail-closed，但真实 baseline contracts 尚未全部 ready。replacement A 不得进入 M1E approval 或 M2 replay；M2 eligible 仍为 0。
