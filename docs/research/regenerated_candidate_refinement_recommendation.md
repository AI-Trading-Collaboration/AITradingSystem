# Regenerated Candidate Refinement Recommendation

最后更新：2026-06-30

- next_task_recommendation: `TRADING-2287_Candidate_Generator_Refinement_Plan`
- candidate_recommendation_count: `3`
- recommendations remain research-only and cannot emit promotion, paper-shadow, production, or broker-ready states.
- promotion_allowed: `false`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`

## Recommendations

|candidate_id|primary_reason|secondary_reasons|next_action|priority|
|---|---|---|---|---|
|`baseline_plus_trend_structure`|`LOW_CONFIDENCE_SIGNAL`|`['HORIZON_MISMATCH', 'ASSET_SPECIFIC_MIXED_RESULTS', 'REGIME_SPECIFIC_ONLY', 'FALSE_RISK_ON_COST_TOO_HIGH', 'DIRECTION_MAPPING_WEAK']`|`REFINE_CONFIDENCE_SCALING`|`LOW`|
|`risk_appetite`|`LOW_CONFIDENCE_SIGNAL`|`['HORIZON_MISMATCH', 'ASSET_SPECIFIC_MIXED_RESULTS', 'REGIME_SPECIFIC_ONLY', 'FALSE_RISK_ON_COST_TOO_HIGH', 'DIRECTION_MAPPING_WEAK']`|`REFINE_CONFIDENCE_SCALING`|`LOW`|
|`volatility_regime`|`LOW_CONFIDENCE_SIGNAL`|`['HORIZON_MISMATCH', 'ASSET_SPECIFIC_MIXED_RESULTS', 'REGIME_SPECIFIC_ONLY', 'DIRECTION_MAPPING_WEAK', 'NO_MEASURABLE_EDGE']`|`REFINE_CONFIDENCE_SCALING`|`LOW`|

## Supporting Diagnostics

- false_signal_cost_labels: `{'FALSE_RISK_ON_COST_TOO_HIGH': 2, 'FALSE_SIGNAL_COST_NOT_DOMINANT': 1}`
- overlap_labels: `{'PARTIALLY_REDUNDANT': 1, 'COMPLEMENTARY': 2}`
- data_quality_impact_labels: `{'DATA_QUALITY_NOT_MATERIAL': 3}`
