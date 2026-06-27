# Research Window Extension Adoption Closeout

- 状态：`RESEARCH_WINDOW_EXTENSION_ADOPTION_READY_PROMOTION_BLOCKED`
- 市场周期：`ai_after_chatgpt`
- primary window：`exact_three_asset_validated`
- requested_start：`2021-02-22`
- actual_portfolio_start：`2021-02-22`
- data_quality_contract：`secondary_cross_checked`
- promotion_allowed：`False`
- paper_shadow_allowed：`False`
- production_allowed：`False`
- broker_action：`none`

## 结论

`2021-02-22` 已采用为后续 QQQ / SGOV / TQQQ research 的 primary validated window。`2022-12-01` legacy window 降级为 `LEGACY_COMPARISON_EVIDENCE`，不得作为主 leaderboard、owner decision primary evidence 或 promotion evidence。

## 依据

- primary window status：`PASS`
- 2020 extension status：`PASS_WITH_EXPECTED_SGOV_SECONDARY_GAP_CAVEAT`
- owner requested `2020-05-26`：`metadata_only`
- requested range actual portfolio start：`2020-05-28`
- static rows：`198`
- probe rows：`15`
- actual-path rows：`30`
- primary upper_state count：`855`
- extension upper_state count：`1005`
- legacy upper_state count：`618`
- final status：`WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT_PROMOTION_BLOCKED`

## 安全边界

窗口采用只改变 research interpretation 和 audit discipline，不恢复 dynamic promotion，不进入 paper-shadow，不进入 production，不接 broker。
