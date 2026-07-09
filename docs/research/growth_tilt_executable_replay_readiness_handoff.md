# Growth Tilt Executable Replay Readiness Handoff

```json
{
  "broker_action": "none",
  "candidate_replay_outcome_rechecked": false,
  "executable_replay_readiness_handoff_ready": true,
  "forward_aging_handoff_ready": false,
  "handoff_candidate_count": 3,
  "handoff_candidates": [
    {
      "candidate_id": "recovery_reentry_speedup_guard",
      "eligible_for_forward_aging": false,
      "forward_aging_handoff_key": "TRADING-2439:forward_aging_candidate_pack:recovery_reentry_speedup_guard",
      "outcome_linkage_key": "growth_tilt_pit_replay:recovery_reentry_speedup_guard:1d,5d,10d,20d",
      "replay_outcome_after_remediation": "NOT_RECHECKED",
      "runtime_execution_smoke_check_ref": "TRADING-2438K:runtime_smoke_check:recovery_reentry_speedup_guard"
    },
    {
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "eligible_for_forward_aging": false,
      "forward_aging_handoff_key": "TRADING-2439:forward_aging_candidate_pack:false_risk_off_confirmation_relaxation",
      "outcome_linkage_key": "growth_tilt_pit_replay:false_risk_off_confirmation_relaxation:1d,5d,10d,20d",
      "replay_outcome_after_remediation": "NOT_RECHECKED",
      "runtime_execution_smoke_check_ref": "TRADING-2438K:runtime_smoke_check:false_risk_off_confirmation_relaxation"
    },
    {
      "candidate_id": "missed_upside_reentry_accelerator",
      "eligible_for_forward_aging": false,
      "forward_aging_handoff_key": "TRADING-2439:forward_aging_candidate_pack:missed_upside_reentry_accelerator",
      "outcome_linkage_key": "growth_tilt_pit_replay:missed_upside_reentry_accelerator:1d,5d,10d,20d",
      "replay_outcome_after_remediation": "NOT_RECHECKED",
      "runtime_execution_smoke_check_ref": "TRADING-2438K:runtime_smoke_check:missed_upside_reentry_accelerator"
    }
  ],
  "next_route": "TRADING-2438L_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Runtime_Remediation",
  "production_effect": "none",
  "ready_for_2438l_recheck": true,
  "schema_version": "growth_tilt_executable_replay_readiness_handoff.v1",
  "status": "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY"
}
```
