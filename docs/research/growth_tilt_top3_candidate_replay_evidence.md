# Growth Tilt Top-3 Candidate Replay Evidence

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
  "broker_enabled": false,
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_pass_count": 0,
  "failed_candidates": [],
  "generated_trading_advice": false,
  "paper_shadow_candidate_found": false,
  "paper_shadow_enabled": false,
  "paper_shadow_schedule_enabled": false,
  "passing_candidates": [],
  "pit_replay_recheck_ready": false,
  "portfolio_weight_mutated": false,
  "production_effect": "none",
  "production_enabled": false,
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
  "schema_version": "growth_tilt_top3_candidate_pit_replay_recheck_evidence.v1",
  "selected_candidate_ids": [
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator"
  ],
  "source_evidence_rows": [
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
      "pit_replay_status": "blocked_replay_engine_gap",
      "production_effect": "none",
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
      "pit_replay_status": "blocked_replay_engine_gap",
      "production_effect": "none",
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
      "pit_replay_status": "blocked_replay_engine_gap",
      "production_effect": "none",
      "source_traceability_verified": false,
      "valid_until_boundary_verified": false
    }
  ],
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED"
}
```
