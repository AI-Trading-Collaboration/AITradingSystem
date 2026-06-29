# Regenerated Candidate Inconclusive Diagnostics Report

最后更新：2026-06-30

## Summary

- status: `INCONCLUSIVE_DIAGNOSTICS_READY_PROMOTION_BLOCKED`
- candidate_count: `3`
- input_actual_path_record_count: `95220`
- input_eligible_record_count: `73188`
- 2285 candidate states: all three regenerated candidates remained `ACTUAL_PATH_VALIDATED_INCONCLUSIVE`.
- promotion_allowed: `false`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`

## Candidate Recommendations

|candidate_id|primary_reason|recommended_next_action|priority|
|---|---|---|---|
|`baseline_plus_trend_structure`|`LOW_CONFIDENCE_SIGNAL`|`REFINE_CONFIDENCE_SCALING`|`LOW`|
|`risk_appetite`|`LOW_CONFIDENCE_SIGNAL`|`REFINE_CONFIDENCE_SCALING`|`LOW`|
|`volatility_regime`|`LOW_CONFIDENCE_SIGNAL`|`REFINE_CONFIDENCE_SCALING`|`LOW`|

## Diagnostic Findings

- signal_density_labels: `{'LOW_CONVICTION_SIGNAL_DESIGN': 27}`
- horizon_asset_labels: `{'LOCAL_NEGATIVE_EDGE': 30, 'LOCAL_WEAK_EDGE': 68, 'INSUFFICIENT_LOCAL_SAMPLE': 3, 'MIXED_BY_HORIZON': 1}`
- direction_alignment_labels: `{'NEUTRAL_OVER_COVERS_TREND_WINDOWS': 2, 'RISK_OFF_MISSES_UPSIDE': 5, 'RISK_ON_DRAWDOWN_STRESS_PRONE': 3, 'DIRECTION_ALIGNMENT_MIXED': 1, 'VOLATILITY_COMPRESSION_MISCLASSIFIED': 1}`
- over-neutralization: no group exceeded the neutral dominance threshold unless `POSSIBLE_OVER_NEUTRALIZATION` appears above.
- local weak edge: inspect `LOCAL_WEAK_EDGE` rows before any generator refinement; they are not promotion evidence.

## False Signal Cost

|candidate_id|label|false_risk_on_cost|false_risk_off_cost|cost_asymmetry|
|---|---|---:|---:|---:|
|`baseline_plus_trend_structure`|`FALSE_RISK_ON_COST_TOO_HIGH`|307.612699|107.730372|-199.882327|
|`risk_appetite`|`FALSE_RISK_ON_COST_TOO_HIGH`|299.53245|109.944313|-189.588137|
|`volatility_regime`|`FALSE_SIGNAL_COST_NOT_DOMINANT`|0.0|146.89697|146.89697|

## Candidate Overlap

|candidate_pair|label|value_corr|direction_agreement|
|---|---|---:|---:|
|`baseline_plus_trend_structure / risk_appetite`|`PARTIALLY_REDUNDANT`|0.803595|0.033982|
|`baseline_plus_trend_structure / volatility_regime`|`COMPLEMENTARY`|0.20652|0.009164|
|`risk_appetite / volatility_regime`|`COMPLEMENTARY`|0.109844|0.022146|

## Data Quality Impact

|candidate_id|label|warning_rate|eligible_ratio|score_delta_due_to_warnings|
|---|---|---:|---:|---:|
|`baseline_plus_trend_structure`|`DATA_QUALITY_NOT_MATERIAL`|0.232913|0.767087|0.023868|
|`risk_appetite`|`DATA_QUALITY_NOT_MATERIAL`|0.232913|0.767087|-0.037708|
|`volatility_regime`|`DATA_QUALITY_NOT_MATERIAL`|0.228402|0.771598|0.03552|

## Next Route

- next_task_recommendation: `TRADING-2287_Candidate_Generator_Refinement_Plan`


本报告只诊断 inconclusive 原因和 signal utility；不生成新 signal、不修改 TRADING-2284 artifacts、不重跑 TRADING-2285 actual-path validation，也不代表 owner approval、promotion、paper-shadow、production 或 broker action。
