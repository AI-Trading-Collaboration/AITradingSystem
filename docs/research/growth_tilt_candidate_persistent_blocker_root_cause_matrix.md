# Growth Tilt Candidate Persistent Blocker Root-Cause Matrix

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "forward_aging_handoff_ready": false,
  "next_route": "TRADING-2438K_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Root_Cause_Remediation",
  "paper_shadow_enabled": false,
  "persistent_blocked_candidate_count": 3,
  "production_effect": "none",
  "production_enabled": false,
  "root_cause_matrix_ready": true,
  "rows": [
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "eligible_for_forward_aging": false,
      "paper_shadow_candidate_found": false,
      "persistent_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "production_effect": "none",
      "recommended_next_action": "replay_runtime_materialization_remediation",
      "replay_outcome_after_escalation": "NOT_RECHECKED",
      "root_cause_categories": [
        "candidate_metric_materialization_missing",
        "candidate_evidence_chain_incomplete_despite_closure",
        "candidate_replay_window_unresolvable",
        "candidate_input_spec_semantically_incomplete",
        "outcome_linkage_not_materialized",
        "replay_engine_contract_ready_but_runtime_not_executable"
      ],
      "root_cause_category": "replay_engine_contract_ready_but_runtime_not_executable",
      "root_cause_layer": [
        "metric_materialization",
        "evidence_materialization",
        "candidate_spec",
        "outcome_linkage",
        "engine_runtime"
      ]
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "eligible_for_forward_aging": false,
      "paper_shadow_candidate_found": false,
      "persistent_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "production_effect": "none",
      "recommended_next_action": "replay_runtime_materialization_remediation",
      "replay_outcome_after_escalation": "NOT_RECHECKED",
      "root_cause_categories": [
        "candidate_metric_materialization_missing",
        "candidate_evidence_chain_incomplete_despite_closure",
        "candidate_replay_window_unresolvable",
        "candidate_input_spec_semantically_incomplete",
        "outcome_linkage_not_materialized",
        "replay_engine_contract_ready_but_runtime_not_executable"
      ],
      "root_cause_category": "replay_engine_contract_ready_but_runtime_not_executable",
      "root_cause_layer": [
        "metric_materialization",
        "evidence_materialization",
        "candidate_spec",
        "outcome_linkage",
        "engine_runtime"
      ]
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "eligible_for_forward_aging": false,
      "paper_shadow_candidate_found": false,
      "persistent_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "production_effect": "none",
      "recommended_next_action": "replay_runtime_materialization_remediation",
      "replay_outcome_after_escalation": "NOT_RECHECKED",
      "root_cause_categories": [
        "candidate_metric_materialization_missing",
        "candidate_evidence_chain_incomplete_despite_closure",
        "candidate_replay_window_unresolvable",
        "candidate_input_spec_semantically_incomplete",
        "outcome_linkage_not_materialized",
        "replay_engine_contract_ready_but_runtime_not_executable"
      ],
      "root_cause_category": "replay_engine_contract_ready_but_runtime_not_executable",
      "root_cause_layer": [
        "metric_materialization",
        "evidence_materialization",
        "candidate_spec",
        "outcome_linkage",
        "engine_runtime"
      ]
    }
  ],
  "schema_version": "growth_tilt_candidate_persistent_blocker_root_cause_matrix.v1",
  "status": "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ESCALATION_READY"
}
```
