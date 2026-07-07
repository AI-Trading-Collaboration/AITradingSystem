# Dynamic strategy recombination targeted improvement plan

- status：`DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY`
- candidate：`growth_tilt_lower_turnover_guarded_transfer_v1`
- variant count：`6`

|Candidate|Purpose|Changes|
|---|---|---|
|`growth_tilt_guarded_transfer_time_slice_repair_v1`|improve weak time slices without changing core return engine|tune_reentry_timing, reduce_drawdown_recovery_lag, preserve_valid_until_window, preserve_lower_turnover_guardrail|
|`growth_tilt_guarded_transfer_regime_repair_v1`|improve behavior in weak regimes|condition_growth_tilt_on_trend_confirmed, strengthen_high_volatility_risk_cap, avoid_excessive_risk_off_defensiveness|
|`growth_tilt_guarded_transfer_drawdown_calibrated_v1`|reduce drawdown materiality gap|reduce_growth_tilt_intensity_under_high_volatility, add_drawdown_sensitive_de_risking, preserve_turnover_budget|
|`growth_tilt_guarded_transfer_return_retention_v1`|preserve more raw growth tilt upside while keeping guardrails|relax_guarded_transfer_only_under_trend_confirmed, preserve_lower_turnover_guardrail, preserve_no_stale_signal|
|`growth_tilt_guarded_transfer_valid_until_strict_v1`|strengthen signal validity evidence|strict_signal_expiry, near_expiry_signal_decay, block_stale_signal_carry_forward|
|`growth_tilt_guarded_transfer_balanced_gate_v1`|balanced candidate targeting observation preview gates|moderate_growth_tilt, lower_turnover_guardrail, strict_valid_until, high_volatility_risk_cap, cooldown_balancing|

## Shared constraints

```json
{
  "base_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
  "block_stale_signal_carry_forward": true,
  "broker_action": "none",
  "monthly_rebalance_allowed_for_primary_decision": false,
  "preserve_lower_turnover_guardrail": true,
  "preserve_return_engine": "growth_tilt_engine",
  "preserve_valid_until_window": true,
  "production_effect": "none"
}
```
