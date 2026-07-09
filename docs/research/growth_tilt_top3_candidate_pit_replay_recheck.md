# Growth Tilt Top-3 Candidate PIT Replay Recheck

- task_id：`TRADING-2438C`
- status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED`
- data quality status：`PASS_WITH_WARNINGS`
- PIT replay recheck ready：`False`
- pass / fail / blocked：`0` / `0` / `3`
- next route：`TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure`

TRADING-2438C 在 2438B blocker closure READY 后独立复核 top-3 candidate PIT replay evidence。本任务不启用 paper-shadow，不生成 trading advice，不把 no passing candidate 误标为 2440 promotion review no-candidate。

```json
{
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_pass_count": 0,
  "data_quality_status": "PASS_WITH_WARNINGS",
  "next_route": "TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure",
  "pit_replay_recheck_ready": false,
  "prior_blocker_closure_status": "GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
}
```

## Candidate Replay Summary

```json
{
  "blocked_candidates": [
    {
      "as_of_boundary_verified": false,
      "blocking_gap_ids": [
        "candidate_pit_replay_engine_available",
        "candidate_replay_input_specs_ready",
        "candidate_source_traceability_manifests_ready",
        "candidate_as_of_boundary_specs_ready",
        "candidate_valid_until_boundary_specs_ready",
        "candidate_outcome_linkage_specs_ready"
      ],
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "outcome_linkage_ready": false,
      "pit_replay_passed": false,
      "production_effect": "none",
      "replay_outcome": "blocked",
      "replay_status": "blocked_replay_engine_gap",
      "source_traceability_verified": false,
      "valid_until_boundary_verified": false
    },
    {
      "as_of_boundary_verified": false,
      "blocking_gap_ids": [
        "candidate_pit_replay_engine_available",
        "candidate_replay_input_specs_ready",
        "candidate_source_traceability_manifests_ready",
        "candidate_as_of_boundary_specs_ready",
        "candidate_valid_until_boundary_specs_ready",
        "candidate_outcome_linkage_specs_ready"
      ],
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "outcome_linkage_ready": false,
      "pit_replay_passed": false,
      "production_effect": "none",
      "replay_outcome": "blocked",
      "replay_status": "blocked_replay_engine_gap",
      "source_traceability_verified": false,
      "valid_until_boundary_verified": false
    },
    {
      "as_of_boundary_verified": false,
      "blocking_gap_ids": [
        "candidate_pit_replay_engine_available",
        "candidate_replay_input_specs_ready",
        "candidate_source_traceability_manifests_ready",
        "candidate_as_of_boundary_specs_ready",
        "candidate_valid_until_boundary_specs_ready",
        "candidate_outcome_linkage_specs_ready"
      ],
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "outcome_linkage_ready": false,
      "pit_replay_passed": false,
      "production_effect": "none",
      "replay_outcome": "blocked",
      "replay_status": "blocked_replay_engine_gap",
      "source_traceability_verified": false,
      "valid_until_boundary_verified": false
    }
  ],
  "broker_action": "none",
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_pass_count": 0,
  "failed_candidates": [],
  "next_route": "TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure",
  "paper_shadow_candidate_found": false,
  "passing_candidates": [],
  "production_effect": "none",
  "schema_version": "growth_tilt_top3_candidate_pit_replay_recheck_summary.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED",
  "top3_candidate_count": 3
}
```

## Remaining Recheck Blockers

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

## No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "evidence_gap_count": 1,
  "forward_aging_observation_started": false,
  "forward_aging_observation_written": false,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [
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
  "generated_signal": false,
  "generated_trading_advice": false,
  "no_effect_boundary_ready": true,
  "outcome_binding_executed": false,
  "outcome_store_mutated": false,
  "paper_shadow_candidate_found": false,
  "paper_shadow_enabled": false,
  "paper_shadow_schedule_enabled": false,
  "portfolio_weight_mutated": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_top3_candidate_pit_replay_recheck_no_effect.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
}
```
