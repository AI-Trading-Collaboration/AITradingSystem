# Refined Candidate Regeneration Report

- status: `REFINED_CANDIDATE_ARTIFACTS_READY_REFINED_ACTUAL_PATH_VALIDATION_BLOCKED`
- task_id: `TRADING-2288_REFINED_CANDIDATE_REGENERATION_WITH_ADJUSTED_CONFIDENCE_SCALING`
- TRADING-2287 primary diagnosis: `INSUFFICIENT_HIGH_CONVICTION_RULE`
- TRADING-2288 只执行 refined regeneration；actual-path validation 阻断到 TRADING-2289。
- promotion_allowed: `false`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`

|original_candidate_id|refined_candidate_id|neutral_ratio_original|neutral_ratio_refined|directional_ratio_original|directional_ratio_refined|high_confidence_ratio_original|high_confidence_ratio_refined|
|---|---|---:|---:|---:|---:|---:|---:|
|baseline_plus_trend_structure|baseline_plus_trend_structure_refined_confidence_v1|0.414376|0.456663|0.585624|0.543337|0.0|0.349975|
|risk_appetite|risk_appetite_refined_confidence_v1|0.362638|0.415235|0.637362|0.584765|0.0|0.0|
|volatility_regime|volatility_regime_refined_confidence_v1|0.492584|0.523359|0.507416|0.476641|0.0|0.349988|

TRADING-2289 必须验证 high-confidence alignment 是否改善、false risk-on / false risk-off 成本是否恶化，以及 refined confidence scaling 是否只是放大噪音。
