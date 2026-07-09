# Growth Tilt Candidate-Level PIT Replay Before After

```json
{
  "after": {
    "candidate_level_blocker_count_after": 0,
    "candidate_replay_blocked_count": 3,
    "candidate_replay_fail_count": 0,
    "candidate_replay_pass_count": 0,
    "candidate_replayable_after_closure_count": 3
  },
  "before": {
    "candidate_level_blocker_count_before": 3,
    "candidate_replay_blocked_count": 3,
    "candidate_replay_fail_count": 0,
    "candidate_replay_pass_count": 0
  },
  "broker_action": "none",
  "next_route": "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure",
  "production_effect": "none",
  "rows": [
    {
      "blocker_closed": true,
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "candidate_replay_failed_after_closure": false,
      "candidate_replay_passed_after_closure": false,
      "candidate_replayable_after_closure": true,
      "paper_shadow_candidate_found": false,
      "prior_blocker_categories": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "replay_status_after_closure": "BLOCKED"
    },
    {
      "blocker_closed": true,
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "candidate_replay_failed_after_closure": false,
      "candidate_replay_passed_after_closure": false,
      "candidate_replayable_after_closure": true,
      "paper_shadow_candidate_found": false,
      "prior_blocker_categories": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "replay_status_after_closure": "BLOCKED"
    },
    {
      "blocker_closed": true,
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "candidate_replay_failed_after_closure": false,
      "candidate_replay_passed_after_closure": false,
      "candidate_replayable_after_closure": true,
      "paper_shadow_candidate_found": false,
      "prior_blocker_categories": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "replay_status_after_closure": "BLOCKED"
    }
  ],
  "schema_version": "growth_tilt_candidate_level_pit_replay_blocker_before_after.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY"
}
```
