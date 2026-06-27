# Research Window Extension Owner Brief

## 1. 为什么主窗口切到 2021-02-22？

`2021-02-22` 是当前 QQQ / SGOV / TQQQ 三资产研究中带 secondary cross-check 的 primary validated exact window。它比 `2022-12-01` legacy window 覆盖更多市场状态，同时避免把 `2020-05-28` 到 `2021-02-19` 的 SGOV secondary-source gap 混入主结论。

## 2. 为什么 2022-12 结果被降级？

`2022-12-01 ~ latest` 过度集中在 2023 之后 AI / 科技强趋势阶段。TRADING-1646～1665 的 final status 已经是 `WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT_PROMOTION_BLOCKED`，因此 legacy 结果只能保留为 comparison evidence，不能作为 primary owner decision 或 promotion evidence。

## 3. 为什么 2020-05-28 只能作为 sensitivity？

`2020-05-28` 是三资产 primary price 的共同可交易起点，但 SGOV secondary source 在 `2021-02-22` 前存在缺口。因此这个窗口可以用于 robustness / sensitivity，但必须携带 `sgov_secondary_gap_2020_05_28_to_2021_02_19` caveat。

## 4. 对第一层 up-state learning 有什么影响？

第一层 label / feature / model 默认使用 `2021-02-22` primary window。upper_state 样本从 legacy `618` 增加到 primary `855`，这是样本覆盖改善，但不证明可学；仍必须检查 label disagreement、split coverage、precision/recall 和 frozen-probe actual-path。

## 5. 对第二层 dynamic probe 有什么影响？

return-seeking、risk-on diagnostic、drawdown-control 和 limited-adjustment probes 后续也必须在 primary window 下重新解释。legacy-only edge 需要标记为 `LEGACY_COMPARISON_EVIDENCE` 或 `LEGACY_WINDOW_ONLY_EDGE`，不能直接进入 state-to-portfolio mapping 决策。

## 6. 为什么 promotion 仍 blocked？

窗口采用解决的是研究解释纪律，不是策略晋级证据。当前 legacy overfit 是 promotion blocker；target-path metrics 仍 diagnostic-only；paper-shadow、production 和 broker 仍全部 disabled。
