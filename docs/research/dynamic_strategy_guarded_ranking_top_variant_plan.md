# Dynamic strategy guarded ranking top variant plan

- status：`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY`
- ranking top candidate：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`

## Guardrail references

- `dynamic_regime_overlay_v0_4_lower_turnover`
- `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`

## Variants

|candidate|role|guarded|purpose|
|---|---|---|---|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`base_ranking_top_reference`|`False`|2365 ranking top reference, not guarded|
|`equal_risk_growth_tilt_guarded_turnover_v1`|`guarded_turnover_v1`|`True`|ranking top + max turnover cap + max single-step weight delta|
|`equal_risk_growth_tilt_guarded_cooldown_v1`|`guarded_cooldown_v1`|`True`|ranking top + cooldown / min holding constraints|
|`equal_risk_growth_tilt_guarded_risk_cap_v1`|`guarded_risk_cap_v1`|`True`|ranking top + stricter risk cap in high volatility / risk-off states|
|`equal_risk_growth_tilt_guarded_valid_until_decay_v1`|`guarded_valid_until_decay_v1`|`True`|ranking top + signal decay near valid-until expiry|
|`equal_risk_growth_tilt_lower_turnover_fusion_v1`|`guarded_fusion_v1`|`True`|ranking top return engine + lower-turnover execution guardrails|
|`equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1`|`guarded_fusion_conservative_v1`|`True`|conservative guarded fusion for robustness testing|
