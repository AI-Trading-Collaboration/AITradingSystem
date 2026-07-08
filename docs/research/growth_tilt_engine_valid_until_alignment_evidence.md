# Growth tilt engine valid-until alignment evidence

本文件记录 growth tilt horizon / confidence / expiry 与 valid_until_window 的
alignment evidence，并保留 `growth_tilt_engine_signal_artifact` blocker。

```json
{
  "alignment_rows": [
    {
      "alignment_questions": [
        "what growth_tilt horizon should valid_until derive from",
        "should valid_until shrink for weak confidence or high volatility",
        "should strong growth tilt use longer validity than weak growth tilt",
        "should recovery regimes require more conservative expiry",
        "how should lag reduce executable remaining validity"
      ],
      "alignment_status": "BLOCKED_BY_SOURCE_SIGNAL_ARTIFACT",
      "confidence_to_expiry_mapping_available": true,
      "contract_ready_after_2418": false,
      "growth_tilt_signal_horizon_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
      "growth_tilt_signal_id": "growth_tilt_engine",
      "high_volatility_expiry_adjustment_available": false,
      "horizon_to_valid_until_mapping_available": true,
      "pit_gate_ready_after_2418": false,
      "ready_for_pit_gate_recheck": true,
      "recovery_state_expiry_adjustment_available": false,
      "remaining_gap": "growth_tilt_engine_signal_artifact remains source-traceability blocked; PIT recheck must keep this blocker until signal artifact metadata exists",
      "valid_until_window_source": "not derived from growth tilt horizon"
    }
  ],
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "production_effect": "none",
  "proposed_confidence_to_expiry_mapping": [
    {
      "confidence_band": "LOW_OR_MISSING",
      "expiry_policy": "shorten validity or block until confidence exists"
    },
    {
      "confidence_band": "MEDIUM",
      "expiry_policy": "use base horizon with near-expiry refresh requirement"
    },
    {
      "confidence_band": "HIGH",
      "expiry_policy": "allow base horizon only if replay validates no stale carry"
    }
  ],
  "proposed_horizon_to_valid_until_mapping": [
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "short_growth_tilt",
      "valid_until_rule": "valid_from + short governed horizon"
    },
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "medium_growth_tilt",
      "valid_until_rule": "valid_from + medium governed horizon"
    },
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "persistent_growth_tilt",
      "valid_until_rule": "valid_from + capped persistent horizon"
    }
  ],
  "ready_for_recheck": true,
  "schema_version": "growth_tilt_engine_valid_until_alignment_evidence.v1",
  "source_traceability_still_blocked": [
    "growth_tilt_engine_signal_artifact"
  ]
}
```
