# Candidate 2288 Refined Regeneration Plan

- next_task: `TRADING-2288_Refined_Candidate_Regeneration_with_Adjusted_Confidence_Scaling`
- recommended default scope: `CONFIDENCE_SCALING_ONLY`
- regeneration_executed: `false`
- actual_path_validation_executed: `false`

|candidate_id|implementation_scope|selected_proposal_count|selected_parameter_set_count|generator_files_to_modify|
|---|---|---:|---:|---|
|baseline_plus_trend_structure|CONFIDENCE_SCALING_ONLY|3|9|src/ai_trading_system/baseline_plus_trend_structure_generator.py|
|risk_appetite|CONFIDENCE_SCALING_ONLY|3|9|src/ai_trading_system/risk_appetite_candidate_generator.py|
|volatility_regime|CONFIDENCE_SCALING_ONLY|3|9|src/ai_trading_system/volatility_regime_candidate_generator.py|

TRADING-2288 才允许实现 adjusted confidence scaling 并重新生成 candidate artifacts；2287 不修改 2284 artifacts。
