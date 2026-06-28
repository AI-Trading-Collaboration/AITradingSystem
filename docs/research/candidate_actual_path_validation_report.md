# Candidate-Level Actual-Path Validation

## 摘要

- task_id: `TRADING-2280_FIRST_LAYER_CANDIDATE_LEVEL_ACTUAL_PATH_VALIDATION`; status: `FIRST_LAYER_CANDIDATE_ACTUAL_PATH_VALIDATION_READY_PROMOTION_BLOCKED`
- covered_candidate_count=`6`; candidate_level_actual_path_available_count=`2`; missing_candidate_signal_artifact_count=`4`.
- promotion_allowed=`false`; paper_shadow_allowed=`false`; production_allowed=`false`; broker_action=`none`.

## 候选验证矩阵

| candidate | previous_state | updated_state | utility_rank | utility | primary_risk_flag | continue | expand | validation_status |
|---|---|---|---:|---:|---|---|---|---|
|`wf_378d_initial`|`RESEARCH_ACCEPTED`|`RESEARCH_ACCEPTED`|`2`|`0.041538`|`coverage_rule_not_satisfied`|`True`|`False`|`candidate_level_actual_path_available`|
|`wf_504d_baseline`|`OWNER_REVIEW_REQUIRED`|`OWNER_REVIEW_REQUIRED`|`1`|`0.070283`|`2023_plus_dependency`|`True`|`True`|`candidate_level_actual_path_available`|
|`baseline`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`False`|`False`|`missing_candidate_signal_artifact`|
|`baseline_plus_trend_structure`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`False`|`False`|`missing_candidate_signal_artifact`|
|`risk_appetite`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`False`|`False`|`missing_candidate_signal_artifact`|
|`volatility_regime`|`OFFLINE_VALIDATION_READY`|`INCONCLUSIVE`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`False`|`False`|`missing_candidate_signal_artifact`|

## Actual-Path 指标

| candidate | net_return | excess_return_delta | max_drawdown | Calmar | Sharpe | turnover | net_of_cost_impact | defensive_probe | 2022 | 2023+ |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
|`wf_378d_initial`|`0.124552`|`-0.015996`|`0.086262`|`1.453457`|`1.211746`|`73.268448`|`-0.033103`|`no_major_regression=True`|`covered_2022=True; delta=0.0998`|`depends=False; first=2022-08-22`|
|`wf_504d_baseline`|`0.159078`|`-0.017389`|`0.093198`|`1.72452`|`1.457731`|`75.587344`|`-0.03528`|`no_major_regression=True`|`covered_2022=False; delta=0.220258`|`depends=True; first=2023-02-22`|
|`baseline`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`no_major_regression=missing_candidate_signal_artifact`|`covered_2022=missing_candidate_signal_artifact; delta=missing_candidate_signal_artifact`|`depends=missing_candidate_signal_artifact; first=missing_candidate_signal_artifact`|
|`baseline_plus_trend_structure`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`no_major_regression=missing_candidate_signal_artifact`|`covered_2022=missing_candidate_signal_artifact; delta=missing_candidate_signal_artifact`|`depends=missing_candidate_signal_artifact; first=missing_candidate_signal_artifact`|
|`risk_appetite`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`no_major_regression=missing_candidate_signal_artifact`|`covered_2022=missing_candidate_signal_artifact; delta=missing_candidate_signal_artifact`|`depends=missing_candidate_signal_artifact; first=missing_candidate_signal_artifact`|
|`volatility_regime`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`missing_candidate_signal_artifact`|`no_major_regression=missing_candidate_signal_artifact`|`covered_2022=missing_candidate_signal_artifact; delta=missing_candidate_signal_artifact`|`depends=missing_candidate_signal_artifact; first=missing_candidate_signal_artifact`|

## 证据边界

- 4 个 offline challenger rows 缺少 candidate signal / prediction artifact，因此 reclassify 为 `INCONCLUSIVE`；本报告不把 baseline evidence 复制成 candidate-level actual-path metrics。
- false risk-on/off、beta/TQQQ attribution、benchmark consistency、recovery delay 和 drawdown warning lead time 对已有 frozen policy rows 仍为 unavailable status。

## 产物

- `candidate_actual_path_validation_report`: `D:\Work\AITradingSystem\docs\research\candidate_actual_path_validation_report.md`
- `candidate_actual_path_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\candidate_actual_path_matrix.json`
- `candidate_risk_attribution_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\candidate_risk_attribution_matrix.json`
- `candidate_state_reclassification_report`: `D:\Work\AITradingSystem\docs\research\candidate_state_reclassification_report.md`
- `updated_research_candidate_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\updated_research_candidate_queue.json`
- `updated_owner_review_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\updated_owner_review_queue.json`
- `updated_offline_validation_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_candidate_actual_path_validation\updated_offline_validation_queue.json`
