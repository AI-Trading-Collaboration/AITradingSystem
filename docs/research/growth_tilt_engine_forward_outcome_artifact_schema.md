# Growth Tilt Engine Forward Outcome Artifact Schema

```json
{
  "allowed_classifications": [
    "pass",
    "fail",
    "inconclusive"
  ],
  "allowed_outcome_horizons": [
    "1d",
    "5d",
    "10d",
    "20d"
  ],
  "artifact_mode": "future_observe_only_outcome_binding_boundary",
  "broker_action": "none",
  "forbidden_fields": [
    "target_weight",
    "rebalance_instruction",
    "order_instruction",
    "broker_order",
    "production_action"
  ],
  "generated_signal": false,
  "generated_trading_advice": false,
  "outcome_schema_ready": true,
  "production_effect": "none",
  "required_fields": [
    "outcome_artifact_id",
    "schema_version",
    "source_signal_artifact_id",
    "source_signal_artifact_checksum",
    "engine_id",
    "strategy_id",
    "signal_as_of",
    "signal_known_at",
    "signal_decision_at",
    "signal_valid_until",
    "outcome_horizon",
    "outcome_window_start",
    "outcome_window_end",
    "outcome_bound_at",
    "outcome_status",
    "pass_fail_inconclusive",
    "baseline_id",
    "baseline_return",
    "signal_return",
    "excess_return_vs_baseline",
    "source_traceability",
    "data_quality_gate_status",
    "market_data_as_of",
    "generated_signal",
    "generated_trading_advice",
    "production_effect",
    "broker_action"
  ],
  "schema_version": "growth_tilt_engine_forward_outcome_artifact_schema.v1",
  "status": "GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY_READY"
}
```
