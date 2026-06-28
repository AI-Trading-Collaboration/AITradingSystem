# Owner Review Candidate Tradeoff Summary

| candidate | decision | rationale | next_action | promotion_ready |
|---|---|---|---|---|
|`wf_504d_baseline`|`expand_neighborhood`|actual-path utility 最高，但 first_prediction_date 在 2023-02-22，2022 coverage 缺失；应继续研究邻域而不是 promotion。|`expand_wf_504d_neighborhood_with_2022_coverage_constraint`|`False`|
|`wf_378d_initial`|`continue_research`|utility 低于 wf_504d_baseline，但覆盖 2022 risk-off/recovery，且 defensive probe 无 major regression，适合作为安全基线。|`use_as_safer_first_layer_challenger_family_baseline`|`False`|

## Safety Boundary

这些 decision 是 owner-review research routing，不是 promotion / paper-shadow decision。

## 产物

- `first_layer_boundary_candidate_owner_review`: `D:\Work\AITradingSystem\docs\research\first_layer_boundary_candidate_owner_review.md`
- `boundary_candidate_comparison_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_boundary_owner_review\boundary_candidate_comparison_matrix.json`
- `owner_review_candidate_tradeoff_summary`: `D:\Work\AITradingSystem\docs\research\owner_review_candidate_tradeoff_summary.md`
- `offline_validation_ready_candidate_summary`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_boundary_owner_review\offline_validation_ready_candidate_summary.json`
- `blocked_candidate_failure_reason_summary`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_boundary_owner_review\blocked_candidate_failure_reason_summary.json`
- `recommended_next_experiment_plan`: `D:\Work\AITradingSystem\docs\research\recommended_next_experiment_plan.md`
