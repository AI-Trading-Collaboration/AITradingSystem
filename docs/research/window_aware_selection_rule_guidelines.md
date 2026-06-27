# Window-Aware Selection Rule Guidelines

- 状态：`WINDOW_AWARE_SELECTION_RULE_TEMPLATES_READY_PROMOTION_BLOCKED`
- 配置：`config/research/window_aware_selection_rule_templates.yaml`
- primary window：`exact_three_asset_validated`
- legacy window：`legacy_research_window_2022_12`
- sensitivity window：`exact_three_asset_primary_only_extension`

## 执行前必须声明

每轮 post-window-extension research 必须在执行前写明：

- primary window 通过条件；
- legacy window 只作 comparison / overfit diagnosis 的用途；
- sensitivity extension 触发 blocker 的条件；
- 失败归因标签；
- 禁止事后选择的指标。

## 禁止

- 不得把 `2021-02-22` primary、`2022-12-01` legacy 和 `2020-05-28` sensitivity 混排为无标签 leaderboard。
- 不得用 target-path metrics 通过 first-layer 或 second-layer gate。
- 不得把 legacy-only edge 当作 owner decision primary evidence。
- 不得把 sensitivity extension 的 uncaveated result 当作 promotion evidence。

## 安全边界

这些 templates 只约束 research evidence 的解释方式，不恢复 promotion，不进入 paper-shadow，不进入 production，不接 broker。
