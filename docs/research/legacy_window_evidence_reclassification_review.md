# Legacy Window Evidence Reclassification Review

- 状态：`LEGACY_WINDOW_EVIDENCE_RECLASSIFICATION_READY_PROMOTION_BLOCKED`
- primary window：`exact_three_asset_validated`
- legacy window：`legacy_research_window_2022_12`
- promotion_allowed：`False`
- paper_shadow_allowed：`False`
- production_allowed：`False`
- broker_action：`none`

## 结论

所有基于 `2022-12-01` legacy window 的旧动态策略、first-layer 和 second-layer 结论统一降级为 `LEGACY_COMPARISON_EVIDENCE` 或 `PRIMARY_WINDOW_RETEST_REQUIRED`。这些旧结论可以解释历史研究脉络，但不能作为主 leaderboard、owner decision primary evidence 或 promotion evidence。

## 重分类摘要

- legacy artifacts reviewed：`6`
- legacy comparison evidence：`5`
- primary evidence rows：`1`
- primary-window retest required：`4`
- legacy overfit blocker：`true`

## 关键影响

`first_layer_policy_aware_calibration_final_matrix`、`hierarchical_first_layer_actual_path_matrix` 和 `first_layer_up_state_learning_final_matrix` 保留为 legacy comparison。后续第一层 label / feature / model 和第二层 probe / state-to-portfolio mapping 必须默认使用 `2021-02-22` primary window。
