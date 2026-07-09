# Growth Tilt Post Runtime Candidate Replay Blocker Summary

```json
{
  "broker_action": "none",
  "next_route": "TRADING-2438M_Growth_Tilt_Post_Runtime_Candidate_PIT_Replay_Blocker_Resolution",
  "post_runtime_candidate_replay_blocker_count": 3,
  "post_runtime_candidate_replay_blocker_summary_ready": true,
  "post_runtime_candidate_replay_blockers": [
    {
      "blocker_category": "runtime_metric_values_not_materialized",
      "blocker_reason": "Runtime metric values remain missing or null after 2438K.",
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "production_effect": "none",
      "required_next_action": "Materialize numeric candidate replay metrics before PASS/FAIL evaluation."
    },
    {
      "blocker_category": "runtime_metric_values_not_materialized",
      "blocker_reason": "Runtime metric values remain missing or null after 2438K.",
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "production_effect": "none",
      "required_next_action": "Materialize numeric candidate replay metrics before PASS/FAIL evaluation."
    },
    {
      "blocker_category": "runtime_metric_values_not_materialized",
      "blocker_reason": "Runtime metric values remain missing or null after 2438K.",
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "production_effect": "none",
      "required_next_action": "Materialize numeric candidate replay metrics before PASS/FAIL evaluation."
    }
  ],
  "production_effect": "none",
  "schema_version": "growth_tilt_post_runtime_candidate_replay_blocker_summary.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_BLOCKED"
}
```
