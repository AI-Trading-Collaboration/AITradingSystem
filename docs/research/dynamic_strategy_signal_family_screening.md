# Dynamic strategy signal family screening

- status：`DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY`

|rank|family|best_candidate|decision|avg_score|time|regime|failure|
|---:|---|---|---|---:|---:|---:|---|
|1|`signal_age_valid_until_family`|`dynamic_valid_until_expiry_strict_v1`|`CONTINUE_OPTIMIZATION`|0.523172|0.214285|0.000000|regime_slice_stability_failure|
|2|`turnover_budget_family`|`dynamic_turnover_budgeted_growth_tilt_v1`|`CONTINUE_OPTIMIZATION`|0.383216|0.214285|0.000000|regime_slice_stability_failure|
|3|`volatility_aware_family`|`dynamic_volatility_scaled_growth_tilt_v1`|`CONTINUE_OPTIMIZATION`|0.167943|0.142857|0.000000|time_slice_stability_failure|
|4|`risk_cap_interaction_family`|`dynamic_risk_cap_adaptive_v1`|`CONTINUE_OPTIMIZATION`|0.151322|0.214286|0.000000|time_slice_stability_failure|
|5|`trend_confirmation_family`|`dynamic_trend_confirmed_low_turnover_v1`|`CONTINUE_OPTIMIZATION`|0.123466|0.000000|0.000000|time_slice_stability_failure|
|6|`regime_transition_family`|`dynamic_regime_recovery_confirmation_v1`|`REJECT_FOR_NOW`|-0.067275|0.071429|0.000000|conservative_cost_failure|