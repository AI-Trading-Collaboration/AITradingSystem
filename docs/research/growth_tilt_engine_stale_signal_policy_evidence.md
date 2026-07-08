# Growth tilt engine stale signal policy evidence

本文件记录 no-stale carry-forward、expired signal block、near-expiry handling
和 signal-to-execution lag policy evidence。

```json
{
  "broker_action": "none",
  "deterministic_next_executable_time_policy_ready": true,
  "engine_id": "growth_tilt_engine",
  "policy_evidence_rows": [
    {
      "carry_forward_requires_owner_approval": true,
      "carry_forward_rule": "hold_previous_actual_position",
      "evidence_status": "CLOSED_WITH_EVIDENCE",
      "expired_signal_execution_rule": "BLOCK_EXECUTION",
      "near_expiry_rule": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
      "policy_id": "growth_tilt_engine_valid_until_window_no_stale_policy_v1",
      "remaining_gap": "replay validation and owner review are still required before any blocker downgrade or observation approval",
      "risk_rows": [
        {
          "affected_semantic_or_signal": "no_stale_signal_carry_forward",
          "category": "CARRY_FORWARD_RISK",
          "evidence": "hold_previous_actual_position can carry stale exposure without owner rule",
          "recommended_fix": "block expired carry-forward or require owner-approved rule",
          "remediation_required": true,
          "risk_id": "VUW-STALE-002",
          "severity": "BLOCKING"
        },
        {
          "affected_semantic_or_signal": "signal_to_execution_lag",
          "category": "SIGNAL_TO_EXECUTION_LAG_RISK",
          "evidence": "prior review observed lag_days=1.0; replay contract missing",
          "recommended_fix": "record lag for every signal-to-execution decision",
          "remediation_required": true,
          "risk_id": "VUW-STALE-003",
          "severity": "MATERIAL"
        },
        {
          "affected_semantic_or_signal": "valid_until_window",
          "category": "NEAR_EXPIRY_OVERTRADING_RISK",
          "evidence": "near-expiry signal behavior is not separately validated",
          "recommended_fix": "define near-expiry decay, block, or refresh-required behavior",
          "remediation_required": true,
          "risk_id": "VUW-STALE-004",
          "severity": "MATERIAL"
        },
        {
          "affected_semantic_or_signal": "signal_version",
          "category": "SIGNAL_REFRESH_COLLISION_RISK",
          "evidence": "new signal overlapping old signal lacks deterministic replacement rule",
          "recommended_fix": "prefer newer as-of-safe valid signal and log collision decision",
          "remediation_required": true,
          "risk_id": "VUW-STALE-005",
          "severity": "MATERIAL"
        }
      ],
      "signal_refresh_collision_rule": "USE_NEWER_SIGNAL_IF_AS_OF_SAFE_AND_VALID",
      "stale_after_rule": "BLOCK_OR_DECAY_SIGNAL",
      "validation_plan_stale_replay": [
        "expired signals do not execute",
        "signal-to-execution lag is measured",
        "near-expiry handling is deterministic",
        "carry-forward is logged or blocked"
      ]
    }
  ],
  "production_effect": "none",
  "ready_for_recheck": true,
  "replay_validation_required": true,
  "required_policy_invariants": {
    "carry_forward_requires_explicit_rule": true,
    "expired_signal_cannot_trigger_new_trade": true,
    "missing_valid_until_blocks_dependent_strategy_recheck": true,
    "owner_review_required_for_carry_forward_in_observation_context": true
  },
  "schema_version": "growth_tilt_engine_stale_signal_policy_evidence.v1",
  "signal_to_execution_lag_policy_ready": true,
  "stale_carry_forward_policy_ready": true
}
```
