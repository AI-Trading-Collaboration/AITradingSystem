# Growth Tilt Top-3 Candidate PIT Replay Engine Remediation Evidence

```json
{
  "as_of_boundary_explicit": false,
  "automatic_execution_allowed": false,
  "blocked_by_forward_aging_gate": true,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "candidate_evidence_rows": [
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
  "forward_aging_handoff_ready": false,
  "generated_trading_advice": false,
  "paper_shadow_enabled": false,
  "paper_shadow_schedule_enabled": false,
  "pit_replay_engine_ready": false,
  "pit_replay_evidence_ready": false,
  "portfolio_weight_mutated": false,
  "prior_forward_aging_status": "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE",
  "prior_pit_replay_status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
  "prior_promotion_review_status": "GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE",
  "production_effect": "none",
  "production_enabled": false,
  "registry_catalog_docs_alignment": true,
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
  "remediation_gap_count": 8,
  "remediation_ready": false,
  "schema_version": "growth_tilt_top3_candidate_pit_replay_engine_remediation_evidence.v1",
  "selected_candidate_ids": [
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator"
  ],
  "source_traceability_complete": false,
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED",
  "top3_candidate_ids_present": true,
  "top3_candidate_selection_ready": true,
  "valid_until_boundary_explicit": false
}
```
