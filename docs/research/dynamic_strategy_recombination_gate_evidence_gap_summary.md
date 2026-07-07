# Dynamic strategy recombination gate evidence gap summary

- status：`DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY`
- candidate：`growth_tilt_lower_turnover_guarded_transfer_v1`
- record ready：`True`

|Gap|Status|Retest required|Improvement direction|
|---|---|---|---|
|`time_slice_evidence_gap`|`GAP_REMAINS`|`True`|tune_reentry_timing, reduce_drawdown_recovery_lag, preserve_valid_until_window|
|`regime_expectation_gap`|`GAP_REMAINS`|`True`|condition_growth_tilt_on_trend_confirmed, strengthen_high_volatility_risk_cap, avoid_excessive_risk_off_defensiveness|
|`drawdown_materiality_gap`|`OWNER_JUDGMENT_REQUIRED`|`True`|reduce_growth_tilt_intensity_under_high_volatility, add_drawdown_sensitive_de_risking, preserve_turnover_budget|
|`return_retention_gap`|`ADEQUATE_BUT_MONITOR`|`True`|preserve_more_raw_growth_tilt_upside, relax_guarded_transfer_only_under_trend_confirmed|
|`turnover_cost_evidence_gap`|`GAP_REMAINS`|`True`|repair guarded transfer turnover behavior, keep realistic and conservative cost survival visible|
|`valid_until_stale_signal_gap`|`PASS`|`True`|strict_signal_expiry, near_expiry_signal_decay, block_stale_signal_carry_forward|

## Blocking summary

- time_slice evidence must improve before observation preview
- regime expectation score must improve before observation preview
- drawdown materiality remains owner-review evidence
- turnover guardrail behavior must be repaired or explained
- return retention is adequate but must remain visible during targeted retest
- valid-until evidence currently passes but must be preserved in 2399
