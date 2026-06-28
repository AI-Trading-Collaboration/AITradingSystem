# TRADING-1886: Daily Incremental Refactor Execution Semantics CLI Boundary

最后更新：2026-06-28

## 背景

每日增量重构巡检以 `a502a432d8c3206d5be0326a7f245e1398652a76`
之后的变更为评估范围。增量范围新增 execution semantics、dynamic strategy
actual-path rebacktest、PIT/walk-forward audit、event taxonomy、cost/cash、
stress/regime 和 artifact governance 等研究层。`src/ai_trading_system/cli_commands/research_execution_semantics.py`
作为 CLI adapter 已增长到约 1000 行，并在多个命令中重复执行 date range
解析、`as_of_date` 组装和 AI regime 默认开始日设置。

本任务只整理 execution semantics CLI adapter 内部 helper 边界。外部
`aits research strategies ...` 命令名、参数、默认路径、artifact path、report
schema、data quality gate、AI regime disclosure、status/safety fields 和投资解释必须保持兼容。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|登记重构范围|DONE|task register 和本需求文档记录本轮维护目标、边界、验收标准和 safety impact。|
|整理 CLI adapter helper 边界|DONE|在 `research_execution_semantics.py` 内集中共享日期参数组装和 AI regime 默认开始日逻辑，减少 wrapper 重复；不新增、删除或重命名外部 command。|
|行为兼容验证|DONE|focused execution semantics pytest、CLI help smoke、Ruff、compileall、docs/task consistency 和 `git diff --check` 通过；本轮未生成 cached-data dependent real output，因此无需额外 `aits validate-data`。|

## Guardrails

- 不新增、删除或重命名任何外部 CLI command。
- 不改变 command 参数、默认路径、artifact path、report schema、status enum、
  safety fields、data quality gate、AI regime disclosure 或 fail-closed 语义。
- 不改变 threshold、score band、promotion gate、position constraint、
  backtest acceptance rule、market-regime interpretation 或投资解释。
- 不写 production weights、active shadow weights、paper account state、broker order
  或 trading action。
- 如果验证发现 execution semantics CLI 输出或 safety boundary 发生语义变化，停止并按
  no-silent-workaround 流程记录 blocker，不提交未验证重构。

## 进展记录

- 2026-06-28: 新增任务并进入 `IN_PROGRESS`。本轮维护目标是整理
  execution semantics CLI adapter 的重复 wrapper/helper 边界，降低
  `research_execution_semantics.py` 后续继续膨胀的维护风险；预期无外部行为变化。
- 2026-06-28: 实现完成并转入 `DONE`。`research_execution_semantics.py`
  新增 `_date_range_kwargs` / `_as_of_kwargs`，并复用
  `DEFAULT_AI_REGIME_BACKTEST_START`，移除 wrapper 内重复 date range parsing 与
  `date(2022, 12, 1)` 默认开始日组装。外部 command surface、参数、默认路径、
  artifact path、status/safety fields 和投资解释保持兼容；验证通过 focused
  xdist pytest、4 个 CLI help smoke、Ruff、compileall、docs/task consistency、
  docs freshness 和 `git diff --check`。
