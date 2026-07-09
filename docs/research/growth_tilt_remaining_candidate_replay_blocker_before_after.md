# Growth Tilt Remaining Candidate Replay Blocker Before After

```json
{
  "after": {
    "candidate_recheckable_after_closure_count": 3,
    "candidate_replay_blocked_count": 3,
    "candidate_replay_fail_count": 0,
    "candidate_replay_pass_count": 0,
    "remaining_candidate_blocker_count_after": 0,
    "replay_outcome_after_closure": "NOT_RECHECKED"
  },
  "before": {
    "candidate_replay_blocked_count": 3,
    "candidate_replay_fail_count": 0,
    "candidate_replay_pass_count": 0,
    "remaining_candidate_blocker_count_before": 3
  },
  "broker_action": "none",
  "next_route": "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Remaining_Blocker_Closure",
  "production_effect": "none",
  "rows": [
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "candidate_recheckable_after_closure": true,
      "closure_result": "CLOSED",
      "paper_shadow_candidate_found": false,
      "prior_blocker_category": "unresolved_replay_execution_result",
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "remaining_blocker_after_closure": null,
      "replay_outcome_after_closure": "NOT_RECHECKED"
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "candidate_recheckable_after_closure": true,
      "closure_result": "CLOSED",
      "paper_shadow_candidate_found": false,
      "prior_blocker_category": "unresolved_replay_execution_result",
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "remaining_blocker_after_closure": null,
      "replay_outcome_after_closure": "NOT_RECHECKED"
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "candidate_recheckable_after_closure": true,
      "closure_result": "CLOSED",
      "paper_shadow_candidate_found": false,
      "prior_blocker_category": "unresolved_replay_execution_result",
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "remaining_blocker_after_closure": null,
      "replay_outcome_after_closure": "NOT_RECHECKED"
    }
  ],
  "schema_version": "growth_tilt_remaining_candidate_replay_blocker_before_after.v1",
  "status": "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY"
}
```
