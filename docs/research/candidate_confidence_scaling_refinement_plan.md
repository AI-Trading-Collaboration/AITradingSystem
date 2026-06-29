# Candidate Confidence Scaling Refinement Plan

- status: `CONFIDENCE_SCALING_REFINEMENT_PLAN_READY_PROMOTION_BLOCKED`
- task_id: `TRADING-2287_CANDIDATE_GENERATOR_CONFIDENCE_SCALING_REFINEMENT_PLAN`
- candidate_count: `3`
- input_prediction_outcome_record_count: `95220`
- input_eligible_record_count: `73188`
- proposal_count: `9`
- parameter_set_count: `27`
- next_task: `TRADING-2288_Refined_Candidate_Regeneration_with_Adjusted_Confidence_Scaling`

TRADING-2287 只生成 confidence scaling refinement plan，不执行 regeneration、不重跑 actual-path validation、不做 promotion、paper-shadow、production 或 broker action。

|candidate_id|dominant_failure_mode|high_confidence_ratio|proposals|retarget_method|
|---|---|---:|---:|---|
|baseline_plus_trend_structure|INSUFFICIENT_HIGH_CONVICTION_RULE|0.0|3|piecewise_direction_aware_trend_scaling|
|risk_appetite|INSUFFICIENT_HIGH_CONVICTION_RULE|0.0|3|core_proxy_agreement_weighted_confidence_scaling|
|volatility_regime|INSUFFICIENT_HIGH_CONVICTION_RULE|0.0|3|cap_relaxation_plus_volatility_percentile_scaling|

Parameter grid 上限为每个 candidate `24` 组；本次实际生成 `27` 组。所有参数仅是 TRADING-2288 refinement design，不是验证通过或上线资格。
