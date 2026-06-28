# Active Selection Policy v2

## 摘要

- task_id: `TRADING-2277_SPLIT_ACTIVE_SELECTION_AND_PROMOTION_POLICY`; status: `FIRST_LAYER_ACTIVE_SELECTION_POLICY_V2_READY_PROMOTION_BLOCKED`
- active selection 只决定 research / owner-review / offline-validation / blocked queues。
- promotion_allowed=`false`; paper_shadow_allowed=`false`; production_allowed=`false`; broker_action=`none`.

## State Counts

| state | count |
|---|---:|
|`RESEARCH_ACCEPTED`|1|
|`OWNER_REVIEW_REQUIRED`|1|
|`OFFLINE_VALIDATION_READY`|4|
|`BLOCKED`|4|
|`INCONCLUSIVE`|0|
|`PROMOTION_READY`|0|

## Policy Semantics

- `RESEARCH_ACCEPTED` 是 research queue state，不等于 promotion。
- `OWNER_REVIEW_REQUIRED` 是 owner-review queue state，不等于 `BLOCKED`。
- `OFFLINE_VALIDATION_READY` 是 offline validation queue state，不等于 paper-shadow。
- `PROMOTION_READY` 只保留给未来独立 promotion gate，本批 count=`0`。
- Ranked review queue 按 actual-path utility 排序；其它风险字段只展示，未加未校准权重。

## Boundary Candidates

| candidate | utility | gate_policy_v2_state | policy_v2_state | expected | pass |
|---|---:|---|---|---|---|
|`wf_378d_initial`|0.041538|`ACCEPTED`|`RESEARCH_ACCEPTED`|`RESEARCH_ACCEPTED`|`True`|
|`wf_504d_baseline`|0.070283|`OWNER_REVIEW_REQUIRED`|`OWNER_REVIEW_REQUIRED`|`OWNER_REVIEW_REQUIRED`|`True`|

## Ranked Review Queue

| rank | candidate | state | utility | 2023+ dependency | beta delta | TQQQ delta | risk flags |
|---:|---|---|---:|---|---:|---:|---|
|1|`wf_504d_baseline`|`OWNER_REVIEW_REQUIRED`|0.070283|`True`|n/a|n/a|2023_plus_dependency, coverage_rule_not_satisfied|
|2|`wf_378d_initial`|`RESEARCH_ACCEPTED`|0.041538|`False`|n/a|n/a|coverage_rule_not_satisfied|

## Offline Validation Queue

| rank | candidate | state | tradeoff summary |
|---:|---|---|---|
|1|`baseline`|`OFFLINE_VALIDATION_READY`|offline_challenger_experiment_only_not_promotion|
|2|`baseline_plus_trend_structure`|`OFFLINE_VALIDATION_READY`|offline_challenger_experiment_only_not_promotion|
|3|`risk_appetite`|`OFFLINE_VALIDATION_READY`|offline_challenger_experiment_only_not_promotion|
|4|`volatility_regime`|`OFFLINE_VALIDATION_READY`|offline_challenger_experiment_only_not_promotion|

## Owner Review Queue

| rank | candidate | utility | risk flags | tradeoff summary |
|---:|---|---:|---|---|
|1|`wf_504d_baseline`|0.070283|2023_plus_dependency, coverage_rule_not_satisfied|候选有较高 actual-path utility，但 gate policy v2 将其风险定义为 owner review，而不是 hard block。|

## 产物

- `active_selection_policy_v2_markdown`: `D:\Work\AITradingSystem\docs\research\active_selection_policy_v2.md`
- `active_selection_policy_v2_yaml`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\active_selection_policy_v2.yaml`
- `research_candidate_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\research_candidate_queue.json`
- `owner_review_queue`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\owner_review_queue.json`
- `promotion_boundary_report`: `D:\Work\AITradingSystem\docs\research\promotion_boundary_report.md`
- `updated_challenger_selection_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_policy_v2\updated_challenger_selection_matrix.json`
