# Growth Tilt Repeated Closure Failure Summary

```json
{
  "broker_action": "none",
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_pass_count": 0,
  "closure_history": {
    "broker_action": "none",
    "candidate_level_blocker_closure_ready": true,
    "output_completeness_closure_ready": true,
    "pit_replay_engine_blocker_closure_ready": true,
    "production_effect": "none",
    "remaining_blocker_closure_ready": true,
    "repeated_closure_attempt_count": 4
  },
  "closure_history_confirmed": true,
  "next_route": "TRADING-2438K_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Root_Cause_Remediation",
  "persistent_blocked_candidate_count": 3,
  "production_effect": "none",
  "repeated_closure_failure_summary_ready": true,
  "schema_version": "growth_tilt_repeated_closure_failure_summary.v1",
  "status": "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ESCALATION_READY",
  "why_previous_closures_were_insufficient": [
    "Prior closure artifacts reached READY, but the latest candidate replay output still has replay_status=BLOCKED and lacks materialized executable replay metrics or outcome evidence.",
    "Prior closure artifacts reached READY, but the latest candidate replay output still has replay_status=BLOCKED and lacks materialized executable replay metrics or outcome evidence.",
    "Prior closure artifacts reached READY, but the latest candidate replay output still has replay_status=BLOCKED and lacks materialized executable replay metrics or outcome evidence."
  ]
}
```
