# Growth Tilt Engine Observe-Only Signal Artifact Schema

```json
{
  "artifact_mode": "observe_only_boundary",
  "broker_action": "none",
  "field_contract": {
    "as_of": "decision date for a future observe-only artifact",
    "decision_at": "timestamp at which observe-only state is evaluated",
    "known_at": "timestamp at which every input is known",
    "manual_review_required": "true for every observe-only signal artifact",
    "source_traceability": "source artifact lineage and checksum evidence",
    "valid_until": "exclusive signal validity boundary"
  },
  "forbidden_fields": [
    "target_weight",
    "rebalance_instruction",
    "order_instruction",
    "broker_order",
    "production_action"
  ],
  "generated_signal": false,
  "next_route": "TRADING-2429_Growth_Tilt_Engine_Forward_Outcome_Binding_Boundary",
  "observe_only": true,
  "production_effect": "none",
  "required_fields": [
    "signal_artifact_id",
    "schema_version",
    "engine_id",
    "strategy_id",
    "as_of",
    "known_at",
    "decision_at",
    "valid_until",
    "valid_until_reason",
    "source_traceability",
    "pit_gate_ready",
    "contract_ready",
    "manual_review_required",
    "observe_only",
    "generated_signal",
    "generated_trading_advice",
    "production_effect",
    "broker_action"
  ],
  "schema_version": "growth_tilt_engine_observe_only_signal_artifact_schema.v1",
  "signal_artifact_instance_generated": false,
  "signal_artifact_schema_ready": true,
  "status": "GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_READY"
}
```