# 判断置信度仓位约束修复

状态：DONE

最后更新：2026-05-08

关联任务：`SCORE-006`

## 背景

2026-05-08 复核最近几天日报时发现，最终 AI 仓位建议连续维持
`40%-40%`。代码检查确认该稳定结果主要来自估值快照复核：
`EXTREME_OVERHEATED` 触发 `valuation` position gate，并按
`config/scoring_rules.yaml` 把风险资产内 AI 仓位上限压到 `40%`。
这部分符合当前配置预期。

但日报同时输出了“置信度调整后建议仓位”低于最终仓位的情况，例如
判断置信度中等时先显示约 `34%-34%`，随后最终仓位显示 `40%-40%`。
根因是判断置信度只在报告层按最终仓位另算展示值，没有进入
`position_gate` 的最严格上限链路。这样会让读者误以为低置信度已经把
最终仓位压低，但最终结果又反向高于该中间值。

## 设计决策

- 判断置信度调整仓位应基于评分模型原始仓位区间计算，属于
  `Raw Position` 与 `Risk Caps` 之间的约束。
- 判断置信度约束必须作为独立 `PositionGate` 进入最终仓位计算，不能只作为
  报告展示值。
- 最终仓位仍使用现有 `min(cap)` 生产规则；估值、风险事件、thesis、
  组合限制、风险预算、数据质量和置信度中谁更严格，谁决定最终上限。
- 低置信度不能释放风险，也不能把未确认风险解释为低风险；本次修复只把
  已有置信度扣减落到仓位上限路径，不引入新的数据源、权重或临时绕行。

## 实施步骤

1. 将 `DailyConfidenceAssessment` 从按最终报告懒计算改为在
   `build_daily_score_report` 中基于组件、质量门禁、市场特征和人工复核状态
   一次性计算。
2. 用评分模型原始仓位计算 `confidence_adjusted_risk_asset_ai_band`。
3. 生成 `confidence` position gate，并与现有 `build_position_gates` 输出一起传入
   `WeightedScoreModel.recommend`。
4. 更新日报文案、`score_architecture`、系统流图和测试，确保字段含义一致。

## 验收标准

- 无更严格风险 gate 时，置信度中/低会把最终仓位上限压到
  `confidence_adjusted_risk_asset_ai_band.max_position`。
- 有更严格风险 gate 时，最终仓位可以低于置信度调整仓位，但不得高于任一已触发
  gate 的上限。
- `scores_daily.csv`、日报、decision snapshot、belief_state 和回测使用相同的
  `confidence_assessment` 对象，不再出现报告层二次计算导致的矛盾。
- 测试覆盖置信度 gate、估值 gate 更严格场景、CSV 字段和报告文案。

## 状态记录

- 2026-05-08：新增并进入实现。原因：日报运行观察暴露置信度调整仓位与最终仓位
  语义冲突，需要把置信度约束接入正式仓位 gate 链路。
- 2026-05-08：完成实现并通过验证。`DailyConfidenceAssessment` 现在在
  `build_daily_score_report` 内固定计算，置信度调整仓位基于评分模型原始仓位，
  并通过 `confidence` position gate 进入最终仓位上限。默认 live
  `score-daily --as-of 2026-05-08` 额外验证在 10 分钟超时内未返回，已终止残留
  进程，未作为通过证据；代码级验证通过 `ruff check src tests`、相关日报/回测测试
  和完整 `pytest -q`。
