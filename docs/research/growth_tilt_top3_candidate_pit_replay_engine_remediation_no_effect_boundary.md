# Growth Tilt Top-3 Candidate PIT Replay Engine Remediation No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "evidence_gap_count": 8,
  "forward_aging_observation_started": false,
  "forward_aging_observation_written": false,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_engine_gap",
      "evidence": {
        "current_engine_available": false,
        "prior_status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
      },
      "gap": "candidate_pit_replay_engine_available did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_pit_replay_engine_available"
    },
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_input_gap",
      "evidence": {
        "candidate_replay_input_specs_ready": false
      },
      "gap": "candidate_replay_input_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_replay_input_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_source_traceability_gap",
      "evidence": {
        "verified_count": 0
      },
      "gap": "source_traceability_complete did not pass.",
      "production_effect": "none",
      "requirement_id": "source_traceability_complete"
    },
    {
      "broker_action": "none",
      "classification": "candidate_as_of_boundary_gap",
      "evidence": {
        "verified_count": 0
      },
      "gap": "as_of_boundary_explicit did not pass.",
      "production_effect": "none",
      "requirement_id": "as_of_boundary_explicit"
    },
    {
      "broker_action": "none",
      "classification": "candidate_valid_until_boundary_gap",
      "evidence": {
        "verified_count": 0
      },
      "gap": "valid_until_boundary_explicit did not pass.",
      "production_effect": "none",
      "requirement_id": "valid_until_boundary_explicit"
    },
    {
      "broker_action": "none",
      "classification": "candidate_outcome_linkage_gap",
      "evidence": {
        "ready_count": 0
      },
      "gap": "outcome_linkage_complete did not pass.",
      "production_effect": "none",
      "requirement_id": "outcome_linkage_complete"
    },
    {
      "broker_action": "none",
      "classification": "pit_replay_evidence_gap",
      "evidence": {
        "evidence_matches_candidates": true,
        "pit_candidates_tested": 0,
        "pit_replay_blocked_count": 3,
        "pit_replay_evidence_ready": true,
        "pit_replay_executed": false
      },
      "gap": "pit_replay_evidence_complete did not pass.",
      "production_effect": "none",
      "requirement_id": "pit_replay_evidence_complete"
    },
    {
      "broker_action": "none",
      "classification": "candidate_to_forward_aging_handoff_gap",
      "evidence": {
        "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
        "pit_candidates_tested": 0,
        "pit_replay_blocked_count": 3
      },
      "gap": "forward_aging_handoff_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "forward_aging_handoff_ready"
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
  "schema_version": "growth_tilt_top3_candidate_pit_replay_engine_remediation_no_effect.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED"
}
```
