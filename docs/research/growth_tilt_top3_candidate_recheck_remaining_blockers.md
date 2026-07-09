# Growth Tilt Top-3 Candidate Recheck Remaining Blockers

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "next_route": "TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "remaining_recheck_blocker_count": 1,
  "remaining_recheck_blocker_summary_ready": true,
  "remaining_recheck_blockers": [
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
  "schema_version": "growth_tilt_top3_candidate_pit_replay_recheck_remaining_blockers.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
}
```
