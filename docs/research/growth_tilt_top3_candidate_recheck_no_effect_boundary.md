# Growth Tilt Top-3 Candidate Recheck No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "evidence_gap_count": 1,
  "forward_aging_observation_started": false,
  "forward_aging_observation_written": false,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [
    {
      "blocker_id": "candidate_replay_outputs",
      "broker_action": "none",
      "classification": "candidate_replay_output_gap",
      "evidence": {
        "blocked_count": 3,
        "failed_count": 0,
        "passing_count": 0,
        "pit_replay_executed": false
      },
      "gap": "candidate_replay_outputs_complete did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_replay_outputs_complete"
    }
  ],
  "generated_signal": false,
  "generated_trading_advice": false,
  "no_effect_boundary_ready": true,
  "outcome_binding_executed": false,
  "outcome_store_mutated": false,
  "paper_shadow_candidate_found": false,
  "paper_shadow_enabled": false,
  "paper_shadow_schedule_enabled": false,
  "portfolio_weight_mutated": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_top3_candidate_pit_replay_recheck_no_effect.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
}
```
