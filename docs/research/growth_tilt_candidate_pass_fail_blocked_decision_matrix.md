# Growth Tilt Candidate Pass Fail Blocked Decision Matrix

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_pass_count": 0,
  "decision_matrix_ready": true,
  "next_route": "TRADING-2438F_Growth_Tilt_Top3_Candidate_Level_PIT_Replay_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "rows": [
    {
      "blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "fail_reason": null,
      "forward_aging_eligible": false,
      "paper_shadow_candidate_found": false,
      "pass_reason": null,
      "production_effect": "none",
      "replay_status": "BLOCKED"
    },
    {
      "blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "fail_reason": null,
      "forward_aging_eligible": false,
      "paper_shadow_candidate_found": false,
      "pass_reason": null,
      "production_effect": "none",
      "replay_status": "BLOCKED"
    },
    {
      "blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "fail_reason": null,
      "forward_aging_eligible": false,
      "paper_shadow_candidate_found": false,
      "pass_reason": null,
      "production_effect": "none",
      "replay_status": "BLOCKED"
    }
  ],
  "schema_version": "growth_tilt_candidate_pass_fail_blocked_decision_matrix.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS"
}
```
