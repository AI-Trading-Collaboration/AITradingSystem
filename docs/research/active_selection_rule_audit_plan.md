# Active Selection Rule 审计计划

- task_id: `TRADING-2276_FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT`
- current_active_selection_accept_count: `0`
- gate policy v2 放行或 owner-review state 不等于 promotion allowed。

## Ablation Modes

- `no_active_selection`
- `relaxed_active_selection`
- `current_active_selection`
- `strict_active_selection`

## Metrics

- `accepted_candidate_count`
- `owner_review_required_count`
- `rejected_candidate_counterfactual_utility`
- `best_rejected_candidate_utility`
- `false_risk_on_delta`
- `false_risk_off_delta`
- `drawdown_delta`
- `turnover_delta`
- `benchmark_consistency_delta`

## Safety Boundary

promotion_allowed=`false`; paper_shadow_allowed=`false`; production_allowed=`false`; broker_action=`none`.
