# Growth Tilt Top-3 Candidate PIT Replay Recheck After Output Closure

- task_id：`TRADING-2438E`
- status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS`
- data quality status：`PASS_WITH_WARNINGS`
- output records complete：`True`
- pass / fail / blocked：`0` / `0` / `3`
- candidate-level blocker count：`3`
- next route：`TRADING-2438F_Growth_Tilt_Top3_Candidate_Level_PIT_Replay_Blocker_Closure`

TRADING-2438E 在 2438D output completeness READY 后复核 candidate level replay status。当前若 candidate 仍为 BLOCKED，必须 route 到 2438F blocker closure；不得把 BLOCKED 误标为 FAIL 或 no-candidate，不得启用 paper-shadow / production / broker。

```json
{
  "candidate_level_blocker_count": 3,
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_output_record_count": 3,
  "candidate_replay_outputs_complete": true,
  "candidate_replay_pass_count": 0,
  "data_quality_status": "PASS_WITH_WARNINGS",
  "forward_aging_candidate_count": 0,
  "forward_aging_handoff_ready": false,
  "next_route": "TRADING-2438F_Growth_Tilt_Top3_Candidate_Level_PIT_Replay_Blocker_Closure",
  "pit_replay_recheck_after_output_closure_ready": false,
  "prior_status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS"
}
```

## Candidate-Level Blockers

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

## Decision Matrix

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

## Forward-Aging Handoff Gate

```json
{
  "blocked_candidate_count": 3,
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_tracking_started": false,
  "forward_aging_candidate_count": 0,
  "forward_aging_candidates": [],
  "forward_aging_handoff_ready": false,
  "forward_aging_observation_started": false,
  "forward_aging_observation_written": false,
  "handoff_policy": "PASS_ONLY_AND_NO_BLOCKED_CANDIDATES",
  "next_route": "TRADING-2438F_Growth_Tilt_Top3_Candidate_Level_PIT_Replay_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_candidate_forward_aging_handoff_gate.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS"
}
```
