# Dynamic strategy stale signal risk audit

- status：`DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN_READY`
- risk count：`8`

{
  "blocking_risk_count": 4,
  "broker_action": "none",
  "input_under_review": "valid_until_window",
  "production_effect": "none",
  "risks": [
    {
      "affected_semantic_or_signal": "valid_until_window",
      "category": "VALID_UNTIL_UNGROUNDED",
      "evidence": "valid_until exists as policy window but is not grounded per signal",
      "recommended_fix": "emit valid_until from generated_at, valid_from and horizon",
      "remediation_required": true,
      "risk_id": "VUW-STALE-001",
      "severity": "BLOCKING"
    },
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
    },
    {
      "affected_semantic_or_signal": "growth_tilt_engine / regime context",
      "category": "STALE_REGIME_LABEL_RISK",
      "evidence": "growth tilt horizon and regime context are not tied to expiry",
      "recommended_fix": "bind regime label timestamp and validity to signal contract",
      "remediation_required": true,
      "risk_id": "VUW-STALE-006",
      "severity": "MATERIAL"
    },
    {
      "affected_semantic_or_signal": "growth_tilt_engine_signal_artifact",
      "category": "VALID_FROM_MISSING_RISK",
      "evidence": "standalone signal artifact lacks valid_from",
      "recommended_fix": "emit valid_from as generated_at or next executable time",
      "remediation_required": true,
      "risk_id": "VUW-STALE-007",
      "severity": "BLOCKING"
    },
    {
      "affected_semantic_or_signal": "growth_tilt_engine_signal_artifact",
      "category": "VALID_UNTIL_MISSING_RISK",
      "evidence": "standalone signal artifact lacks valid_until; prior stale_count=0",
      "recommended_fix": "emit valid_until and stale_after on every signal record",
      "remediation_required": true,
      "risk_id": "VUW-STALE-008",
      "severity": "BLOCKING"
    }
  ],
  "schema_version": "dynamic_strategy_stale_signal_risk_audit.v1"
}
