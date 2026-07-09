# Growth Tilt Top-3 Candidate PIT Replay Recheck After Remaining Blocker Closure

- task_id: `TRADING-2438I`
- status: `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_BLOCKED`
- data quality status: `PASS_WITH_WARNINGS`
- replay recheck handoff ready: `True`
- pass / fail / blocked: `0` / `0` / `3`
- persistent blocker count: `3`
- forward-aging handoff ready: `False`
- next route: `TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation`

TRADING-2438I 在 2438H remaining blocker closure READY 后重新判定 top-3 candidate 的 PASS / FAIL / BLOCKED。若任一 candidate 仍为 `BLOCKED`，结果必须保持 BLOCKED 并进入 2438J；只有至少 1 个 PASS 且没有 BLOCKED 时才进入 forward-aging。该任务不启用 paper-shadow、production 或 broker，也不生成交易建议。

```json
{
  "candidate_recheckable_after_closure_count": 3,
  "candidate_replay_blocked_count": 3,
  "candidate_replay_fail_count": 0,
  "candidate_replay_output_record_count": 3,
  "candidate_replay_pass_count": 0,
  "data_quality_status": "PASS_WITH_WARNINGS",
  "forward_aging_candidate_count": 0,
  "forward_aging_handoff_ready": false,
  "next_route": "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation",
  "persistent_candidate_replay_blocker_count": 3,
  "prior_status": "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY",
  "remaining_candidate_blocker_closure_ready": true,
  "remaining_candidate_blocker_count_after": 0,
  "replay_recheck_handoff_ready": true,
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_BLOCKED"
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
  "next_route": "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "rows": [
    {
      "baseline_id": "growth_tilt_current_policy_baseline",
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:recovery_reentry_speedup_guard",
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
      "paper_shadow_enabled": false,
      "pass_reason": null,
      "persistent_blocker_category": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "persistent_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "production_effect": "none",
      "replay_status": "BLOCKED",
      "source_refs": {
        "input_spec_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\input_specs.json#recovery_reentry_speedup_guard",
        "outcome_linkage_key": "growth_tilt_pit_replay:recovery_reentry_speedup_guard:1d,5d,10d,20d",
        "source_traceability_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\source_traceability_manifest.json#recovery_reentry_speedup_guard",
        "valid_until_policy_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\valid_until_boundary_manifest.json#recovery_reentry_speedup_guard"
      },
      "source_replay_status": "blocked_replay_engine_gap"
    },
    {
      "baseline_id": "growth_tilt_current_policy_baseline",
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:false_risk_off_confirmation_relaxation",
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
      "paper_shadow_enabled": false,
      "pass_reason": null,
      "persistent_blocker_category": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "persistent_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "production_effect": "none",
      "replay_status": "BLOCKED",
      "source_refs": {
        "input_spec_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\input_specs.json#false_risk_off_confirmation_relaxation",
        "outcome_linkage_key": "growth_tilt_pit_replay:false_risk_off_confirmation_relaxation:1d,5d,10d,20d",
        "source_traceability_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\source_traceability_manifest.json#false_risk_off_confirmation_relaxation",
        "valid_until_policy_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\valid_until_boundary_manifest.json#false_risk_off_confirmation_relaxation"
      },
      "source_replay_status": "blocked_replay_engine_gap"
    },
    {
      "baseline_id": "growth_tilt_current_policy_baseline",
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:missed_upside_reentry_accelerator",
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
      "paper_shadow_enabled": false,
      "pass_reason": null,
      "persistent_blocker_category": [
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "invalid_valid_until_policy",
        "missing_outcome_linkage",
        "replay_engine_execution_gap"
      ],
      "persistent_blocker_reason": "Candidate remains BLOCKED by: candidate_pit_replay_engine_available, candidate_replay_input_specs_ready, candidate_source_traceability_manifests_ready, candidate_as_of_boundary_specs_ready, candidate_valid_until_boundary_specs_ready, candidate_outcome_linkage_specs_ready",
      "production_effect": "none",
      "replay_status": "BLOCKED",
      "source_refs": {
        "input_spec_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\input_specs.json#missed_upside_reentry_accelerator",
        "outcome_linkage_key": "growth_tilt_pit_replay:missed_upside_reentry_accelerator:1d,5d,10d,20d",
        "source_traceability_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\source_traceability_manifest.json#missed_upside_reentry_accelerator",
        "valid_until_policy_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_pit_replay_engine_blocker_closure\\valid_until_boundary_manifest.json#missed_upside_reentry_accelerator"
      },
      "source_replay_status": "blocked_replay_engine_gap"
    }
  ],
  "schema_version": "growth_tilt_candidate_recheck_after_remaining_blocker_decision_matrix.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_BLOCKED"
}
```

## Forward-Aging Handoff

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
  "next_route": "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_candidate_forward_aging_after_remaining_blocker_handoff_summary.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_BLOCKED"
}
```

## Persistent Candidate Replay Blockers

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "next_route": "TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation",
  "paper_shadow_enabled": false,
  "persistent_candidate_replay_blocker_count": 3,
  "persistent_candidate_replay_blocker_summary_ready": true,
  "persistent_candidate_replay_blockers": [
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
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:recovery_reentry_speedup_guard",
      "evidence_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#recovery_reentry_speedup_guard",
      "production_effect": "none",
      "remaining_blocker_closure_result": "CLOSED",
      "replay_outcome_after_remaining_blocker_closure": "NOT_RECHECKED",
      "required_next_action": "Escalate through TRADING-2438J persistent candidate PIT replay blocker review before any forward-aging handoff."
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
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:false_risk_off_confirmation_relaxation",
      "evidence_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#false_risk_off_confirmation_relaxation",
      "production_effect": "none",
      "remaining_blocker_closure_result": "CLOSED",
      "replay_outcome_after_remaining_blocker_closure": "NOT_RECHECKED",
      "required_next_action": "Escalate through TRADING-2438J persistent candidate PIT replay blocker review before any forward-aging handoff."
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
      "closure_evidence_ref": "TRADING-2438H:remaining_candidate_blocker_closure:missed_upside_reentry_accelerator",
      "evidence_ref": "D:\\Work\\AITradingSystem\\outputs\\research_strategies\\growth_tilt_top3_candidate_pit_replay\\pit_replay_evidence.json#missed_upside_reentry_accelerator",
      "production_effect": "none",
      "remaining_blocker_closure_result": "CLOSED",
      "replay_outcome_after_remaining_blocker_closure": "NOT_RECHECKED",
      "required_next_action": "Escalate through TRADING-2438J persistent candidate PIT replay blocker review before any forward-aging handoff."
    }
  ],
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_persistent_candidate_replay_blocker_summary.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_BLOCKED"
}
```
