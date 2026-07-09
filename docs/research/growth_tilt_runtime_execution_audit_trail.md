# Growth Tilt Runtime Execution Audit Trail

```json
{
  "audit_records": [
    {
      "broker_action": "none",
      "candidate_id": "recovery_reentry_speedup_guard",
      "candidate_replay_outcome_rechecked": false,
      "deterministic_runtime_output_supported": true,
      "production_effect": "none",
      "runtime_entrypoint_ref": "TRADING-2438K:replay_runtime_entrypoint_shell",
      "runtime_execution_smoke_check_ref": "TRADING-2438K:runtime_smoke_check:recovery_reentry_speedup_guard",
      "runtime_execution_smoke_check_status": "PASS",
      "runtime_input_ref": "TRADING-2438K:runtime_input:recovery_reentry_speedup_guard"
    },
    {
      "broker_action": "none",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "candidate_replay_outcome_rechecked": false,
      "deterministic_runtime_output_supported": true,
      "production_effect": "none",
      "runtime_entrypoint_ref": "TRADING-2438K:replay_runtime_entrypoint_shell",
      "runtime_execution_smoke_check_ref": "TRADING-2438K:runtime_smoke_check:false_risk_off_confirmation_relaxation",
      "runtime_execution_smoke_check_status": "PASS",
      "runtime_input_ref": "TRADING-2438K:runtime_input:false_risk_off_confirmation_relaxation"
    },
    {
      "broker_action": "none",
      "candidate_id": "missed_upside_reentry_accelerator",
      "candidate_replay_outcome_rechecked": false,
      "deterministic_runtime_output_supported": true,
      "production_effect": "none",
      "runtime_entrypoint_ref": "TRADING-2438K:replay_runtime_entrypoint_shell",
      "runtime_execution_smoke_check_ref": "TRADING-2438K:runtime_smoke_check:missed_upside_reentry_accelerator",
      "runtime_execution_smoke_check_status": "PASS",
      "runtime_input_ref": "TRADING-2438K:runtime_input:missed_upside_reentry_accelerator"
    }
  ],
  "broker_action": "none",
  "next_route": "TRADING-2438L_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Runtime_Remediation",
  "production_effect": "none",
  "runtime_execution_audit_trail_ready": true,
  "schema_version": "growth_tilt_runtime_execution_audit_trail.v1",
  "status": "GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY"
}
```
