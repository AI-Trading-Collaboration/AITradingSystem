# Growth Tilt Candidate-Level PIT Replay Blocker Closure Records

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_level_blocker_closure_records_ready": true,
  "candidate_level_blocker_count_after": 0,
  "candidate_level_blocker_count_before": 3,
  "next_route": "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "records": [
    {
      "after_state": {
        "blocker_after_state": "CLOSED_FOR_2438G_RECHECK",
        "candidate_replayable_after_closure": true,
        "next_recheck_route": "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure",
        "replay_status_after_closure": "BLOCKED"
      },
      "blocker_closed": true,
      "broker_action": "none",
      "broker_order_generated": false,
      "candidate_id": "recovery_reentry_speedup_guard",
      "candidate_replay_failed_after_closure": false,
      "candidate_replay_passed_after_closure": false,
      "candidate_replayable_after_closure": true,
      "closure_action_taken": "Closed candidate-level replayability blocker by binding candidate input, source traceability, as-of, valid-until, outcome linkage and handoff refs for TRADING-2438G independent recheck; replay outcome remains undecided.",
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:recovery_reentry_speedup_guard",
      "closure_evidence_refs": [
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\input_specs.json#recovery_reentry_speedup_guard",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\source_traceability_manifest.json#recovery_reentry_speedup_guard",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#recovery_reentry_speedup_guard",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\valid_until_boundary_manifest.json#recovery_reentry_speedup_guard",
        "growth_tilt_pit_replay:recovery_reentry_speedup_guard:1d,5d,10d,20d",
        "TRADING-2439:forward_aging_candidate_pack:recovery_reentry_speedup_guard",
        "TRADING-2438F:closure_record:recovery_reentry_speedup_guard"
      ],
      "paper_shadow_candidate_found": false,
      "portfolio_weight_mutated": false,
      "prior_blocker_categories": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "prior_blocker_category": "missing_metric_summary",
      "prior_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "remaining_blocker_reason": null,
      "replay_status_after_closure": "BLOCKED",
      "required_next_action": "Close TRADING-2438F candidate-level PIT replay blockers before forward-aging handoff.",
      "trading_advice_generated": false
    },
    {
      "after_state": {
        "blocker_after_state": "CLOSED_FOR_2438G_RECHECK",
        "candidate_replayable_after_closure": true,
        "next_recheck_route": "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure",
        "replay_status_after_closure": "BLOCKED"
      },
      "blocker_closed": true,
      "broker_action": "none",
      "broker_order_generated": false,
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "candidate_replay_failed_after_closure": false,
      "candidate_replay_passed_after_closure": false,
      "candidate_replayable_after_closure": true,
      "closure_action_taken": "Closed candidate-level replayability blocker by binding candidate input, source traceability, as-of, valid-until, outcome linkage and handoff refs for TRADING-2438G independent recheck; replay outcome remains undecided.",
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:false_risk_off_confirmation_relaxation",
      "closure_evidence_refs": [
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\input_specs.json#false_risk_off_confirmation_relaxation",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\source_traceability_manifest.json#false_risk_off_confirmation_relaxation",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#false_risk_off_confirmation_relaxation",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\valid_until_boundary_manifest.json#false_risk_off_confirmation_relaxation",
        "growth_tilt_pit_replay:false_risk_off_confirmation_relaxation:1d,5d,10d,20d",
        "TRADING-2439:forward_aging_candidate_pack:false_risk_off_confirmation_relaxation",
        "TRADING-2438F:closure_record:false_risk_off_confirmation_relaxation"
      ],
      "paper_shadow_candidate_found": false,
      "portfolio_weight_mutated": false,
      "prior_blocker_categories": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "prior_blocker_category": "missing_metric_summary",
      "prior_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "remaining_blocker_reason": null,
      "replay_status_after_closure": "BLOCKED",
      "required_next_action": "Close TRADING-2438F candidate-level PIT replay blockers before forward-aging handoff.",
      "trading_advice_generated": false
    },
    {
      "after_state": {
        "blocker_after_state": "CLOSED_FOR_2438G_RECHECK",
        "candidate_replayable_after_closure": true,
        "next_recheck_route": "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure",
        "replay_status_after_closure": "BLOCKED"
      },
      "blocker_closed": true,
      "broker_action": "none",
      "broker_order_generated": false,
      "candidate_id": "missed_upside_reentry_accelerator",
      "candidate_replay_failed_after_closure": false,
      "candidate_replay_passed_after_closure": false,
      "candidate_replayable_after_closure": true,
      "closure_action_taken": "Closed candidate-level replayability blocker by binding candidate input, source traceability, as-of, valid-until, outcome linkage and handoff refs for TRADING-2438G independent recheck; replay outcome remains undecided.",
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:missed_upside_reentry_accelerator",
      "closure_evidence_refs": [
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\input_specs.json#missed_upside_reentry_accelerator",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\source_traceability_manifest.json#missed_upside_reentry_accelerator",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#missed_upside_reentry_accelerator",
        "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\valid_until_boundary_manifest.json#missed_upside_reentry_accelerator",
        "growth_tilt_pit_replay:missed_upside_reentry_accelerator:1d,5d,10d,20d",
        "TRADING-2439:forward_aging_candidate_pack:missed_upside_reentry_accelerator",
        "TRADING-2438F:closure_record:missed_upside_reentry_accelerator"
      ],
      "paper_shadow_candidate_found": false,
      "portfolio_weight_mutated": false,
      "prior_blocker_categories": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "prior_blocker_category": "missing_metric_summary",
      "prior_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "prior_replay_status": "BLOCKED",
      "production_effect": "none",
      "remaining_blocker_reason": null,
      "replay_status_after_closure": "BLOCKED",
      "required_next_action": "Close TRADING-2438F candidate-level PIT replay blockers before forward-aging handoff.",
      "trading_advice_generated": false
    }
  ],
  "schema_version": "growth_tilt_candidate_level_pit_replay_blocker_closure_records.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY"
}
```
