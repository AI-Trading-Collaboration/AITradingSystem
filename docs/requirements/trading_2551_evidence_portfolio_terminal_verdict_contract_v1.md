# TRADING-2551 Evidence Portfolio Terminal Verdict Contract V1

## 背景

`TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1` 已按冻结 admission 完成一次 bounded
`DATA_RESEARCH` confirmation，并产生技术有效的 `RETAIN` aggregate result。现有
`EvidenceFirstResearchPortfolio` 共享 contract 只允许 `UNRESOLVED`，Atlas reader 也把
“尚未判定”硬编码为唯一展示，因此无法在不绕过 contract 的前提下接纳终态结果。

本任务是最小 serial contract wave。它只扩展共享状态机与确定性展示语义，不修改
TRADING-2550 的数值、阈值、reducer precedence 或 admission，也不触发新的数据访问、
DQ、confirmation、backtest、QuantConnect、provider 或交易动作。

## 冻结设计

1. `EvidenceState` 增加 `RETAIN`、`REJECT`、`INSUFFICIENT` 三个 terminal verdict。
2. `current_verdict` 允许 `UNRESOLVED` 及上述三个 terminal verdict。
3. `next_experiment` 与 verdict 精确配对：
   - `UNRESOLVED` -> `FROZEN_SIGNAL_VALUE_CONFIRMATION`
   - `RETAIN` -> `OWNER_REVIEW_CONDITIONAL_OPTIONS_PAIRED_COMPARISON`
   - `REJECT` -> `OPTIONS_IMPLEMENTATION_P0_CLOSED`
   - `INSUFFICIENT` -> `EXPLICIT_PROSPECTIVE_EVIDENCE_ONLY`
4. evidence ladder 的 `SIGNAL_VALUE` 节点状态必须等于 `current_verdict`；其余冻结节点不变。
5. Atlas reader 从 contract 状态确定性渲染 verdict，不再硬编码“尚未判定”。

## 实施步骤

1. 扩展 contract enum、跨字段 invariant 与失败信息。
2. 扩展 Atlas presentation mapping 和动态 verdict 文案。
3. 增加四种 verdict 的 contract/renderer 测试及非法配对 negative tests。
4. 更新 `docs/system_flow.md`，运行 focused、impact、architecture、contract、integration、
   reproducibility 与 Full 验证。
5. 经 publication fence 集成并发布到 `main`；随后 TRADING-2550 从该精确新基线完成结果接入。

## 验收标准

- 四个 verdict 及其 exact next-action 配对均可构造并确定性渲染。
- 任意 verdict/next-action 错配或 ladder 不一致均 fail closed。
- 现有 `UNRESOLVED` 配置保持向后兼容。
- 不修改 TRADING-2550 aggregate result、DQ/replay receipts 或 frozen admission。
- `production_effect=none`，`broker_action=none`，所有外部研究和交易动作计数为 0。

## 状态

- 2026-09-01：因 TRADING-2550 的技术有效 `RETAIN` 结果暴露共享 contract 表达缺口，创建最小 serial contract wave；状态 `IN_PROGRESS`。
- 2026-09-01：共享 enum、verdict/next-action/ladder invariant、Atlas 动态渲染与页面来源分类已实现；focused Atlas validation 为 17 passed。任务进入 `BASELINE_DONE`，待正式验证和 publication fence 收口后供 TRADING-2550 从新 exact `main` 基线接入结果。
- 2026-09-01：首次正式 architecture-fitness 为 877 passed / 5 failed；失败仅来自新增 canonical task 的冻结计数，以及 `docs/system_flow.md` 变更后的 architecture manifests 与 append-only compatibility authority 尚未刷新。任务暂回 `IN_PROGRESS` 执行 deterministic generator refresh；contract 语义测试未失败，也未重跑任何数据或 confirmation。
- 2026-09-01：首次 Full 为 10,046 passed / 34 failed / 3 skipped。32 项由 canonical task requirement ref 未提取及 `report-flow-authority` 尚未刷新共同引起，2 项为 16-worker 资源压力下的 `MemoryError`。失败 Full `full_20260831T195432Z` 已作为唯一 parent 保全；修复仅刷新绑定与派生 authority，后续 Full 仍并行但降低 worker 数，不重跑 confirmation。
- 2026-09-01：确定性 failure-fix 已全部收敛，最终候选的 architecture-fitness 为 882 passed；两项资源压力测试也已在 2-worker parallel 下单独 PASS。任务恢复 `BASELINE_DONE`，将以首次失败 Full 为 parent、8-worker 对最终候选执行正式 Full rerun；该收口不读取市场数据，也不消耗第二次 confirmation。
