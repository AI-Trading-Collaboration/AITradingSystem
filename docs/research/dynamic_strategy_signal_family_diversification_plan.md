# Dynamic strategy signal family diversification plan

## Summary

- status：`DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN_READY`
- all families preserve `valid_until_window`：`true`
- all families require pre-registered hypotheses：`true`

## Signal families

### regime_transition_family

- purpose：improve behavior around risk-off to recovery transitions
- candidate templates：
- `dynamic_regime_reentry_accelerated_v1`
- `dynamic_regime_recovery_confirmation_v1`
- `dynamic_regime_risk_off_exit_decay_v1`
- hypotheses：
- `recovery re-entry lag is a major source of return gap`
- `controlled re-entry can improve upside without exploding turnover`
- `risk-off exit should be gradual but not excessively slow`
- required guardrails：
- `valid_until_window`
- `max_single_step_weight_delta`
- `cooldown_limited_event_driven_comparison`
- `risk_cap_preserved`

### trend_confirmation_family

- purpose：allow growth tilt only under confirmed trend conditions
- candidate templates：
- `dynamic_trend_confirmed_growth_tilt_v1`
- `dynamic_trend_confirmed_low_turnover_v1`
- `dynamic_ai_trend_confirmed_guarded_v1`
- hypotheses：
- `ranking top return advantage may come from growth tilt`
- `growth tilt should be gated by trend confirmation`
- `false risk-on can be reduced with confirmation filters`
- required guardrails：
- `no_stale_signal_execution`
- `volatility_state_check`
- `turnover_cap`
- `conservative_cost_stress`

### volatility_aware_family

- purpose：improve high-volatility drawdown behavior while preserving low-volatility upside
- candidate templates：
- `dynamic_volatility_scaled_growth_tilt_v1`
- `dynamic_volatility_floor_adjusted_v1`
- `dynamic_high_vol_risk_cap_strict_v1`
- hypotheses：
- `high volatility phases need stricter risk cap`
- `low volatility phases can allow modest upside capture`
- `volatility-aware scaling may reduce drawdown without fully suppressing returns`
- required guardrails：
- `high_volatility_slice_test`
- `low_volatility_slice_test`
- `drawdown_gap_check`
- `cost_adjusted_result`

### signal_age_valid_until_family

- purpose：tune signal decay and expiry behavior
- candidate templates：
- `dynamic_signal_age_decay_v1`
- `dynamic_valid_until_expiry_strict_v1`
- `dynamic_valid_until_near_expiry_de_risk_v1`
- hypotheses：
- `valid-until window is necessary but may need decay tuning`
- `near-expiry signals should have reduced weight impact`
- `stale signal carry-forward must remain forbidden`
- required guardrails：
- `no_signal_after_expiry`
- `stale_signal_execution_count`
- `valid_until_window_preserved`
- `signal_to_execution_lag_days`

### turnover_budget_family

- purpose：convert turnover control into an explicit optimization constraint
- candidate templates：
- `dynamic_turnover_budgeted_growth_tilt_v1`
- `dynamic_turnover_budgeted_regime_overlay_v1`
- `dynamic_turnover_budgeted_reentry_v1`
- hypotheses：
- `turnover budget may improve robustness better than ad-hoc cooldown`
- `explicit turnover caps can preserve cost-adjusted edge`
- `dynamic budget allocation can allow selective high-conviction changes`
- required guardrails：
- `max_turnover_per_month`
- `max_single_step_weight_delta`
- `transaction_cost_drag`
- `turnover_adjusted_score`

### risk_cap_interaction_family

- purpose：test whether risk cap is too strict or too loose under different signals
- candidate templates：
- `dynamic_risk_cap_adaptive_v1`
- `dynamic_risk_cap_trend_conditioned_v1`
- `dynamic_risk_cap_recovery_relaxed_v1`
- hypotheses：
- `current risk cap may suppress recovery upside`
- `adaptive risk cap may preserve downside protection while improving re-entry`
- `trend-conditioned risk cap may reduce false risk-off`
- required guardrails：
- `drawdown_not_materially_worse`
- `high_volatility_behavior_not_degraded`
- `downside_capture_check`
- `owner_review_trigger_if_drawdown_worse`


## Next route

`TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening`
