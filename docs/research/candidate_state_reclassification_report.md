# Candidate State Reclassification Report

## 状态重分类矩阵

| candidate | previous_state | updated_state | stability | reason | continue_research | expand_neighborhood | promotion_ready |
|---|---|---|---|---|---|---|---|
|`wf_378d_initial`|`RESEARCH_ACCEPTED`|`RESEARCH_ACCEPTED`|`stable`|`state_stable_after_candidate_level_actual_path_validation`|`True`|`False`|`False`|
|`wf_504d_baseline`|`OWNER_REVIEW_REQUIRED`|`OWNER_REVIEW_REQUIRED`|`stable`|`state_stable_after_candidate_level_actual_path_validation`|`True`|`True`|`False`|
|`baseline`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`unstable_missing_candidate_signal_artifact`|`offline_ready_reclassified_missing_candidate_signal_artifact`|`False`|`False`|`False`|
|`baseline_plus_trend_structure`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`unstable_missing_candidate_signal_artifact`|`offline_ready_reclassified_missing_candidate_signal_artifact`|`False`|`False`|`False`|
|`risk_appetite`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`unstable_missing_candidate_signal_artifact`|`offline_ready_reclassified_missing_candidate_signal_artifact`|`False`|`False`|`False`|
|`volatility_regime`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`unstable_missing_candidate_signal_artifact`|`offline_ready_reclassified_missing_candidate_signal_artifact`|`False`|`False`|`False`|

## Boundary

`RESEARCH_ACCEPTED` / `OWNER_REVIEW_REQUIRED` / `OFFLINE_VALIDATION_READY` 都不是 promotion、paper-shadow 或 production state。

## 产物

- `candidate_actual_path_validation_report`: `D:\Work\AITradingSystem\docs\research\candidate_actual_path_validation_report.md`
- `candidate_actual_path_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\candidate_actual_path_matrix.json`
- `candidate_risk_attribution_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\candidate_risk_attribution_matrix.json`
- `candidate_state_reclassification_report`: `D:\Work\AITradingSystem\docs\research\candidate_state_reclassification_report.md`
- `updated_research_candidate_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\updated_research_candidate_queue.json`
- `updated_owner_review_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\updated_owner_review_queue.json`
- `updated_offline_validation_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\updated_offline_validation_queue.json`
