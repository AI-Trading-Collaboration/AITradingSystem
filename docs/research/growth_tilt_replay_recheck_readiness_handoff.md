# Growth Tilt Replay Recheck Readiness Handoff

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_recheckable_after_closure_count": 3,
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_pass_count": 0,
  "forward_aging_handoff_ready": false,
  "handoff_policy": "RECHECK_ONLY_2438I_DECIDES_PASS_FAIL_BLOCKED",
  "next_route": "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Remaining_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "recheckable_candidates": [
    {
      "candidate_id": "recovery_reentry_speedup_guard",
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:recovery_reentry_speedup_guard",
      "next_recheck_route": "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Remaining_Blocker_Closure",
      "replay_outcome_after_closure": "NOT_RECHECKED"
    },
    {
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:false_risk_off_confirmation_relaxation",
      "next_recheck_route": "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Remaining_Blocker_Closure",
      "replay_outcome_after_closure": "NOT_RECHECKED"
    },
    {
      "candidate_id": "missed_upside_reentry_accelerator",
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:missed_upside_reentry_accelerator",
      "next_recheck_route": "TRADING-2438I_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Remaining_Blocker_Closure",
      "replay_outcome_after_closure": "NOT_RECHECKED"
    }
  ],
  "replay_recheck_handoff_ready": true,
  "schema_version": "growth_tilt_replay_recheck_readiness_handoff.v1",
  "status": "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY"
}
```
