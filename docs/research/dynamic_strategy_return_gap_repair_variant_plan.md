# 动态策略 return-gap repair variant plan

- status：`DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY`
- ranking top reference：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`

## Return gap components

|component|assessment|variant|retest|
|---|---|---|---|
|`upside_capture_gap`|`repairable_with_moderate_turnover_increase`|`upside_capture_guarded_v1`|`True`|
|`risk_on_weight_gap`|`repairable_with_moderate_turnover_increase`|`return_gap_repair_fusion_v1`|`True`|
|`reentry_delay_gap`|`repairable_without_turnover_increase`|`reentry_repair_v1`|`True`|
|`excessive_defensive_floor_gap`|`repair_requires_higher_drawdown_risk`|`return_gap_repair_fusion_v1`|`True`|
|`valid_until_expiry_gap`|`repairable_without_turnover_increase`|`valid_until_decay_tuned_v1`|`True`|
|`cooldown_block_gap`|`repairable_without_turnover_increase`|`cooldown_balanced_v1`|`True`|
|`turnover_cap_opportunity_cost`|`not_recommended_to_repair`|`preserve_conservative_turnover_cap`|`False`|
|`growth_tilt_intensity_gap`|`repairable_with_moderate_turnover_increase`|`upside_capture_guarded_v1`|`True`|

## Variants for 2379

|variant|purpose|turnover|cost|priority|
|---|---|---|---|---|
|`dynamic_regime_overlay_v0_4_lower_turnover`|current robustness top reference|`none`|`none`|`reference`|
|`dynamic_regime_overlay_v0_4_reentry_repair_v1`|reduce recovery / trend-confirmed reentry lag|`low_to_moderate`|`low`|`high`|
|`dynamic_regime_overlay_v0_4_upside_capture_guarded_v1`|improve low-volatility and risk-on upside capture|`moderate`|`moderate_cost_stress_required`|`high`|
|`dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1`|reduce stale signal and near-expiry drag|`low`|`low`|`medium`|
|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|reduce missed upside caused by cooldown without overtrading|`low_to_moderate`|`moderate_cost_stress_required`|`medium`|
|`dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1`|combine lower-turnover guardrail with limited growth tilt|`moderate`|`moderate_to_high_cost_stress_required`|`high`|

## Forbidden paths

- `remove_lower_turnover_guardrail_without_replacement`
- `use_monthly_rebalance_as_primary`
- `allow_stale_signal_carry_forward`
- `increase_growth_tilt_without_risk_cap`
- `remove_cooldown_entirely_as_final_candidate`
- `increase_turnover_without_cost_stress`
- `optimize_only_for_total_return`
- `ignore_drawdown_or_regime_slice_failures`