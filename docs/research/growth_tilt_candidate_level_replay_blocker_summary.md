# Growth Tilt Candidate-Level Replay Blocker Summary

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_level_blocker_count": 3,
  "candidate_level_blocker_summary_ready": true,
  "candidate_level_blockers": [
    {
      "blocker_category": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "production_effect": "none",
      "replay_status": "BLOCKED",
      "required_next_action": "Close TRADING-2438F candidate-level PIT replay blockers before forward-aging handoff."
    },
    {
      "blocker_category": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "production_effect": "none",
      "replay_status": "BLOCKED",
      "required_next_action": "Close TRADING-2438F candidate-level PIT replay blockers before forward-aging handoff."
    },
    {
      "blocker_category": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "production_effect": "none",
      "replay_status": "BLOCKED",
      "required_next_action": "Close TRADING-2438F candidate-level PIT replay blockers before forward-aging handoff."
    }
  ],
  "next_route": "TRADING-2438F_Growth_Tilt_Top3_Candidate_Level_PIT_Replay_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_candidate_level_replay_blocker_summary.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS"
}
```
