# Growth Tilt Top-3 Candidate Selection

```json
{
  "broker_action": "none",
  "candidate_limit": 3,
  "evidence_gap_count": 6,
  "pit_candidates_selected": 3,
  "production_effect": "none",
  "schema_version": "growth_tilt_top3_candidate_selection.v1",
  "selected_candidates": [
    {
      "batch_decision": "pit_candidate",
      "broker_action": "none",
      "candidate_family": "recovery_reentry",
      "candidate_id": "recovery_reentry_speedup_guard",
      "pit_replay_passed": false,
      "pit_replay_status": "blocked_replay_engine_gap",
      "production_effect": "none",
      "promotion_review_candidate": false,
      "research_questions": [
        "slow_growth_recovery_reentry",
        "missed_upside_without_drawdown_damage"
      ],
      "selection_rank": 1
    },
    {
      "batch_decision": "pit_candidate",
      "broker_action": "none",
      "candidate_family": "false_risk_off_filter",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "pit_replay_passed": false,
      "pit_replay_status": "blocked_replay_engine_gap",
      "production_effect": "none",
      "promotion_review_candidate": false,
      "research_questions": [
        "over_defensive_entry",
        "false_defensive_day_reduction"
      ],
      "selection_rank": 2
    },
    {
      "batch_decision": "pit_candidate",
      "broker_action": "none",
      "candidate_family": "missed_upside_reentry",
      "candidate_id": "missed_upside_reentry_accelerator",
      "pit_replay_passed": false,
      "pit_replay_status": "blocked_replay_engine_gap",
      "production_effect": "none",
      "promotion_review_candidate": false,
      "research_questions": [
        "slow_growth_recovery_reentry",
        "missed_upside_without_drawdown_damage"
      ],
      "selection_rank": 3
    }
  ],
  "selection_basis": "prior_batch_screen_pit_candidate_order_no_market_ranking",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
  "top3_candidate_selection_ready": true
}
```
