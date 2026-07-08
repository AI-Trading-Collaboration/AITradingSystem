# Growth Tilt Paper-Shadow Candidate No-Effect Boundary

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
      "classification": "forward_aging_gate_gap",
      "evidence": {
        "pit_replay_pass_count": 0,
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
      },
      "gap": "source_2438_pit_replay_ready_for_promotion_review did not pass.",
      "production_effect": "none",
      "requirement_id": "source_2438_pit_replay_ready_for_promotion_review"
    },
    {
      "broker_action": "none",
      "classification": "forward_aging_gate_gap",
      "evidence": {
        "forward_aging_candidate_count": 0,
        "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
        "status": "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE"
      },
      "gap": "source_2439_forward_aging_candidate_pack_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "source_2439_forward_aging_candidate_pack_ready"
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
  "schema_version": "growth_tilt_paper_shadow_candidate_no_effect.v1",
  "status": "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE"
}
```
