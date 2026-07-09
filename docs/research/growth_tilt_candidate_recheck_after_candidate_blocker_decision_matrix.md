# Growth Tilt Candidate Recheck After Candidate Blocker Decision Matrix

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_pass_count": 0,
  "decision_matrix_ready": true,
  "next_route": "TRADING-2438H_Growth_Tilt_Remaining_Candidate_PIT_Replay_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "rows": [
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
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:recovery_reentry_speedup_guard",
      "evidence_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#recovery_reentry_speedup_guard",
      "fail_reason": null,
      "failed_criteria": [],
      "forward_aging_eligible": false,
      "forward_aging_handoff_key": "TRADING-2439:forward_aging_candidate_pack:recovery_reentry_speedup_guard",
      "metric_summary": {
        "false_risk_off_delta": null,
        "max_drawdown_delta_vs_baseline": null,
        "missed_upside_delta": null,
        "return_delta_vs_baseline": null,
        "turnover_delta_vs_baseline": null,
        "whipsaw_delta": null
      },
      "paper_shadow_candidate_found": false,
      "pass_reason": null,
      "production_effect": "none",
      "replay_status": "BLOCKED"
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
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:false_risk_off_confirmation_relaxation",
      "evidence_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#false_risk_off_confirmation_relaxation",
      "fail_reason": null,
      "failed_criteria": [],
      "forward_aging_eligible": false,
      "forward_aging_handoff_key": "TRADING-2439:forward_aging_candidate_pack:false_risk_off_confirmation_relaxation",
      "metric_summary": {
        "false_risk_off_delta": null,
        "max_drawdown_delta_vs_baseline": null,
        "missed_upside_delta": null,
        "return_delta_vs_baseline": null,
        "turnover_delta_vs_baseline": null,
        "whipsaw_delta": null
      },
      "paper_shadow_candidate_found": false,
      "pass_reason": null,
      "production_effect": "none",
      "replay_status": "BLOCKED"
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
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:missed_upside_reentry_accelerator",
      "evidence_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#missed_upside_reentry_accelerator",
      "fail_reason": null,
      "failed_criteria": [],
      "forward_aging_eligible": false,
      "forward_aging_handoff_key": "TRADING-2439:forward_aging_candidate_pack:missed_upside_reentry_accelerator",
      "metric_summary": {
        "false_risk_off_delta": null,
        "max_drawdown_delta_vs_baseline": null,
        "missed_upside_delta": null,
        "return_delta_vs_baseline": null,
        "turnover_delta_vs_baseline": null,
        "whipsaw_delta": null
      },
      "paper_shadow_candidate_found": false,
      "pass_reason": null,
      "production_effect": "none",
      "replay_status": "BLOCKED"
    }
  ],
  "schema_version": "growth_tilt_candidate_recheck_after_candidate_blocker_decision_matrix.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE_BLOCKED"
}
```
