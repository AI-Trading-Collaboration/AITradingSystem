# First-Layer Challenger Matrix v2

## 摘要

- task_id: `TRADING-2278_FIRST_LAYER_CHALLENGER_MATRIX_V2_RERUN`; status: `FIRST_LAYER_CHALLENGER_MATRIX_V2_READY_PROMOTION_BLOCKED`
- gate_policy_v2_applied=`true`; active_selection_policy_v2_applied=`true`.
- promotion_allowed=`false`; paper_shadow_allowed=`false`; production_allowed=`false`; broker_action=`none`.
- research_accepted_count=`1`; owner_review_required_count=`1`; blocked_count=`4`; promotion_ready_count=`0`.

## Best Candidates

| bucket | candidate | state | utility | transition_from_v1 |
|---|---|---|---:|---|
|`research`|`wf_378d_initial`|`RESEARCH_ACCEPTED`|0.041538|`BLOCKED -> RESEARCH_ACCEPTED`|
|`owner_review`|`wf_504d_baseline`|`OWNER_REVIEW_REQUIRED`|0.070283|`BLOCKED -> OWNER_REVIEW_REQUIRED`|
|`blocked`|`wf_expanding_initial`|`BLOCKED`|0.000404|`BLOCKED -> BLOCKED`|

## Boundary Candidate State Transitions

| candidate | v1 state | v2 state | transition | promotion_allowed |
|---|---|---|---|---|
|`wf_378d_initial`|`BLOCKED`|`RESEARCH_ACCEPTED`|`BLOCKED -> RESEARCH_ACCEPTED`|`False`|
|`wf_504d_baseline`|`BLOCKED`|`OWNER_REVIEW_REQUIRED`|`BLOCKED -> OWNER_REVIEW_REQUIRED`|`False`|

## Queues

| queue | count |
|---|---:|
|`research_candidate_queue_v2`|5|
|`owner_review_queue_v2`|1|
|`blocked_candidate_queue_v2`|4|

## 产物

- `first_layer_challenger_matrix_v2`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_challenger_matrix_v2\first_layer_challenger_matrix_v2.json`
- `first_layer_challenger_report_v2`: `D:\Work\AITradingSystem\docs\research\first_layer_challenger_report_v2.md`
- `research_candidate_queue_v2`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_challenger_matrix_v2\research_candidate_queue_v2.json`
- `owner_review_queue_v2`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_challenger_matrix_v2\owner_review_queue_v2.json`
- `blocked_candidate_queue_v2`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_challenger_matrix_v2\blocked_candidate_queue_v2.json`
- `promotion_boundary_check_v2`: `D:\Work\AITradingSystem\docs\research\promotion_boundary_check_v2.md`
