# Growth Tilt Forward Aging No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "evidence_gap_count": 2,
  "forward_aging_observation_started": false,
  "forward_aging_observation_written": false,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [
    {
      "broker_action": "none",
      "classification": "pit_replay_gate_gap",
      "evidence": {
        "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
        "pit_candidates_tested": 0,
        "pit_replay_pass_count": 0,
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
      },
      "gap": "source_2438_top3_pit_replay_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "source_2438_top3_pit_replay_ready"
    },
    {
      "broker_action": "none",
      "classification": "pit_replay_gate_gap",
      "evidence": {
        "pit_replay_pass_candidate_count": 0
      },
      "gap": "pit_replay_pass_candidate_available did not pass.",
      "production_effect": "none",
      "requirement_id": "pit_replay_pass_candidate_available"
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
  "schema_version": "growth_tilt_forward_aging_no_effect.v1",
  "status": "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE"
}
```
