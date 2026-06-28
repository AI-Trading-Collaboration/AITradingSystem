# First-Layer Boundary Candidate Owner Review

## 摘要

- task_id: `TRADING-2279_FIRST_LAYER_BOUNDARY_CANDIDATE_OWNER_REVIEW_PACKAGE`; status: `FIRST_LAYER_BOUNDARY_OWNER_REVIEW_READY_PROMOTION_BLOCKED`
- OWNER_REVIEW_REQUIRED 不等于 promotion；RESEARCH_ACCEPTED 不等于 paper-shadow。
- promotion_allowed=`false`; paper_shadow_allowed=`false`; production_allowed=`false`; broker_action=`none`.

## Boundary Candidate Decisions

| candidate | state | utility | owner_review_decision | primary_risk_flag | utility_tradeoff_acceptable |
|---|---|---:|---|---|---|
|`wf_504d_baseline`|`OWNER_REVIEW_REQUIRED`|0.070283|`expand_neighborhood`|`2023_plus_dependency`|`inconclusive`|
|`wf_378d_initial`|`RESEARCH_ACCEPTED`|0.041538|`continue_research`|`coverage_rule_not_satisfied`|`True`|

## Return / Risk Metrics

| candidate | net_return | excess_return_delta | max_drawdown | Calmar | Sharpe | turnover | net_of_cost_impact |
|---|---:|---:|---:|---:|---:|---:|---:|
|`wf_504d_baseline`|`0.159078`|`-0.017389`|`0.093198`|`1.72452`|`1.457731`|`75.587344`|`-0.03528`|
|`wf_378d_initial`|`0.124552`|`-0.015996`|`0.086262`|`1.453457`|`1.211746`|`73.268448`|`-0.033103`|

## Objective / Dependency Metrics

| candidate | false_risk_on | false_risk_off | defensive_probe | 2022_stress_slice | 2023+ dependency | beta/TQQQ attribution | benchmark_consistency | recovery_delay | drawdown_warning_lead_time |
|---|---|---|---|---|---|---|---|---|---|
|`wf_504d_baseline`|`unavailable_for_frozen_actual_path_policy_rows`|`unavailable_for_frozen_actual_path_policy_rows`|`no_major_regression=True; count=0`|`covered_2022=False; delta=0.220258`|`depends=True; first_prediction=2023-02-22`|`unavailable_candidate_level_beta_tqqq_dependency_not_run; unavailable_candidate_level_beta_tqqq_dependency_not_run`|`unavailable_for_frozen_actual_path_policy_rows`|`unavailable_for_frozen_actual_path_policy_rows`|`unavailable_for_frozen_actual_path_policy_rows`|
|`wf_378d_initial`|`unavailable_for_frozen_actual_path_policy_rows`|`unavailable_for_frozen_actual_path_policy_rows`|`no_major_regression=True; count=0`|`covered_2022=True; delta=0.0998`|`depends=False; first_prediction=2022-08-22`|`unavailable_candidate_level_beta_tqqq_dependency_not_run; unavailable_candidate_level_beta_tqqq_dependency_not_run`|`unavailable_for_frozen_actual_path_policy_rows`|`unavailable_for_frozen_actual_path_policy_rows`|`unavailable_for_frozen_actual_path_policy_rows`|

## Offline Validation Ready Summary

- candidate_count: `4`
- candidate_ids: `baseline`, `baseline_plus_trend_structure`, `risk_appetite`, `volatility_regime`
- common_characteristics: `offline_challenger_experiment_only_not_promotion`, `actual_path_utility_unavailable`, `promotion_allowed_false`, `requires_future_candidate_level_actual_path_backtest`

## Blocked Candidate Failure Reasons

- candidate_count: `4`
- misclassification_check: `no_obvious_misclassification_detected`

| candidate | blocked_reasons | utility | defensive_probe | misclassification_risk |
|---|---|---:|---|---|
|`wf_252d_initial`|`no_major_regression_in_defensive_probe`|`-0.014885`|`False`|`blocked_due_strong_defensive_regression_reasonable`|
|`wf_expanding_initial`|`no_major_regression_in_defensive_probe`|`0.000404`|`False`|`blocked_due_strong_defensive_regression_reasonable`|
|`combined_proxy`|`rsp_to_spy`, `qqqe_to_qqq`|`None`|`None`|`blocked_due_missing_proxy_inputs_reasonable`|
|`equal_cap_weight_divergence`|`rsp_to_spy`, `qqqe_to_qqq`|`None`|`None`|`blocked_due_missing_proxy_inputs_reasonable`|

## 指标说明

false risk-on/off、beta/TQQQ attribution、benchmark consistency、recovery delay 和 drawdown warning lead time 当前没有 candidate-level actual-path 数值；本 package 保留 unavailable status，不补造 0。

## 产物

- `first_layer_boundary_candidate_owner_review`: `D:\Work\AITradingSystem\docs\research\first_layer_boundary_candidate_owner_review.md`
- `boundary_candidate_comparison_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_boundary_owner_review\boundary_candidate_comparison_matrix.json`
- `owner_review_candidate_tradeoff_summary`: `D:\Work\AITradingSystem\docs\research\owner_review_candidate_tradeoff_summary.md`
- `offline_validation_ready_candidate_summary`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_boundary_owner_review\offline_validation_ready_candidate_summary.json`
- `blocked_candidate_failure_reason_summary`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_boundary_owner_review\blocked_candidate_failure_reason_summary.json`
- `recommended_next_experiment_plan`: `D:\Work\AITradingSystem\docs\research\recommended_next_experiment_plan.md`
