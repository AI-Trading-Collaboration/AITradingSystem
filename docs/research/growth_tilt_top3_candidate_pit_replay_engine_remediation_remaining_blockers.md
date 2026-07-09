# Growth Tilt Top-3 Candidate PIT Replay Engine Remaining Blockers

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "next_route": "TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "remaining_blocker_summary_ready": true,
  "remaining_blockers": [
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_engine_gap",
      "evidence": {
        "current_engine_available": false,
        "prior_status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
      },
      "gap": "candidate_pit_replay_engine_available did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_pit_replay_engine_available"
    },
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_input_gap",
      "evidence": {
        "candidate_replay_input_specs_ready": false
      },
      "gap": "candidate_replay_input_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_replay_input_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_source_traceability_gap",
      "evidence": {
        "verified_count": 0
      },
      "gap": "source_traceability_complete did not pass.",
      "production_effect": "none",
      "requirement_id": "source_traceability_complete"
    },
    {
      "broker_action": "none",
      "classification": "candidate_as_of_boundary_gap",
      "evidence": {
        "verified_count": 0
      },
      "gap": "as_of_boundary_explicit did not pass.",
      "production_effect": "none",
      "requirement_id": "as_of_boundary_explicit"
    },
    {
      "broker_action": "none",
      "classification": "candidate_valid_until_boundary_gap",
      "evidence": {
        "verified_count": 0
      },
      "gap": "valid_until_boundary_explicit did not pass.",
      "production_effect": "none",
      "requirement_id": "valid_until_boundary_explicit"
    },
    {
      "broker_action": "none",
      "classification": "candidate_outcome_linkage_gap",
      "evidence": {
        "ready_count": 0
      },
      "gap": "outcome_linkage_complete did not pass.",
      "production_effect": "none",
      "requirement_id": "outcome_linkage_complete"
    },
    {
      "broker_action": "none",
      "classification": "pit_replay_evidence_gap",
      "evidence": {
        "evidence_matches_candidates": true,
        "pit_candidates_tested": 0,
        "pit_replay_blocked_count": 3,
        "pit_replay_evidence_ready": true,
        "pit_replay_executed": false
      },
      "gap": "pit_replay_evidence_complete did not pass.",
      "production_effect": "none",
      "requirement_id": "pit_replay_evidence_complete"
    },
    {
      "broker_action": "none",
      "classification": "candidate_to_forward_aging_handoff_gap",
      "evidence": {
        "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
        "pit_candidates_tested": 0,
        "pit_replay_blocked_count": 3
      },
      "gap": "forward_aging_handoff_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "forward_aging_handoff_ready"
    }
  ],
  "schema_version": "growth_tilt_top3_candidate_pit_replay_engine_remediation_remaining_blockers.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED",
  "unresolved_engine_blocker_count": 8
}
```
