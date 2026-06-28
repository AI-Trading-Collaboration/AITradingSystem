# Promotion Boundary Report

Active selection policy v2 不决定 promotion；promotion gate 仍独立关闭。

- promotion_gate_independent: `True`
- active_selection_can_set_promotion_ready: `False`
- promotion_ready_count: `0`
- promotion_allowed=`false`; paper_shadow_allowed=`false`; production_allowed=`false`; broker_action=`none`.

## Boundary Candidates

| candidate | selection_state | promotion_allowed |
|---|---|---|
|`wf_378d_initial`|`RESEARCH_ACCEPTED`|`False`|
|`wf_504d_baseline`|`OWNER_REVIEW_REQUIRED`|`False`|

## 产物

- `active_selection_policy_v2_markdown`: `D:\Work\AITradingSystem\docs\research\active_selection_policy_v2.md`
- `active_selection_policy_v2_yaml`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\active_selection_policy_v2.yaml`
- `research_candidate_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\research_candidate_queue.json`
- `owner_review_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\owner_review_queue.json`
- `promotion_boundary_report`: `D:\Work\AITradingSystem\docs\research\promotion_boundary_report.md`
- `updated_challenger_selection_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\updated_challenger_selection_matrix.json`
