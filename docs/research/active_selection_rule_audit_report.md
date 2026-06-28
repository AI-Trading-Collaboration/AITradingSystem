# First-Layer Active Selection Rule Audit

## 摘要

- task_id: `TRADING-2276_FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT`; status: `FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT_READY_PROMOTION_BLOCKED`
- current active selection accept count: `0`
- active_selection_marginal_utility: `negative`; recommended_action: `split_selection_and_promotion`
- promotion_allowed=`false`, paper_shadow_allowed=`false`, production_allowed=`false`, broker_action=`none`.
- gate policy v2 evidence source: `regenerated_from_trading_2274_and_2275_code_paths`；ignored `outputs/` artifact 不是唯一 source of truth。

## Ablation Matrix

| mode | accepted | owner_review | blocked | rejected | best_accepted | best_rejected |
|---|---:|---:|---:|---:|---:|---:|
|`no_active_selection`|1|1|2|3|0.041538|0.070283|
|`relaxed_active_selection`|1|1|2|3|0.041538|0.070283|
|`current_active_selection`|0|0|4|4|None|0.070283|
|`strict_active_selection`|0|0|4|4|None|0.070283|

## Boundary Candidates

| policy_id | utility | gate_policy_v2_state | boundary_reason |
|---|---:|---|---|
|`wf_504d_baseline`|0.070283|`OWNER_REVIEW_REQUIRED`|`owner_review_state_suppressed_by_current_selection_boundary`|
|`wf_378d_initial`|0.041538|`ACCEPTED`|`covered_2022_but_current_coverage_pass_rule_false`|

## 关键结论

- current selection 阻断 best actual-path candidate: `True`。
- 与 gate policy v2 分层语义冲突: `True`。
- owner-review candidates suppressed by selection: `1`。

## 产物

- `active_selection_rule_audit_report`: `D:\Work\AITradingSystem\docs\research\active_selection_rule_audit_report.md`
- `active_selection_ablation_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_rule_audit\active_selection_ablation_matrix.json`
- `active_selection_counterfactual_report`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_rule_audit\active_selection_counterfactual_report.json`
- `active_selection_threshold_sensitivity`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_rule_audit\active_selection_threshold_sensitivity.json`
- `active_selection_recommended_policy`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_active_selection_rule_audit\active_selection_recommended_policy.yaml`
