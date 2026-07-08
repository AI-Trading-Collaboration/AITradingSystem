# Growth Tilt Top-3 Candidate PIT Replay No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "evidence_gap_count": 6,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_engine_gap",
      "evidence": {
        "current_engine": null,
        "required_engine": "growth_tilt_candidate_specific_pit_replay_engine"
      },
      "gap": "candidate_pit_replay_engine_available did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_pit_replay_engine_available"
    },
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_input_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_replay_input_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_replay_input_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_source_traceability_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_source_traceability_manifests_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_source_traceability_manifests_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_as_of_boundary_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_as_of_boundary_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_as_of_boundary_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_valid_until_boundary_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_valid_until_boundary_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_valid_until_boundary_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_outcome_linkage_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_outcome_linkage_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_outcome_linkage_specs_ready"
    }
  ],
  "generated_signal": false,
  "generated_trading_advice": false,
  "no_effect_boundary_ready": true,
  "outcome_binding_executed": false,
  "outcome_store_mutated": false,
  "paper_shadow_enabled": false,
  "paper_shadow_schedule_enabled": false,
  "portfolio_weight_mutated": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_top3_candidate_pit_replay_no_effect.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
}
```
