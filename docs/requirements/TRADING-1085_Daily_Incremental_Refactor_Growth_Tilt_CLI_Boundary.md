# TRADING-1085: Daily Incremental Refactor Growth-Tilt CLI Boundary

最后更新：2026-06-26

## 背景

每日增量重构巡检以 `b601cf9860db4b93b495a5dcec24a24b3453d651`
之后的变更为评估范围。增量范围新增 TRADING-1065～1084 equal-risk
growth tilt exploration，同时把 growth-tilt command adapters 继续挂在
`src/ai_trading_system/cli_commands/research_simple_baselines.py`。该模块已从
simple-baseline wrapper 演变为覆盖 simple baseline、Layer-1、Layer-2、
controlled growth 和 growth tilt 的混合适配层，继续膨胀会增加后续 CLI 契约维护
和 review 成本。

本任务只拆分 growth-tilt CLI adapter 边界。外部
`aits research strategies ...` 命令名、参数、artifact path、report schema、
data quality gate、status/safety fields 和投资解释必须保持兼容。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|登记重构范围|DONE|task register 和本需求文档记录本轮维护目标、边界、验收标准和 safety impact。|
|拆分 CLI adapter 边界|DONE|新增 `cli_commands/research_growth_tilt.py`，集中 TRADING-1065～1084 growth-tilt command factories、专用 roadmap command 和 command registry；`research_simple_baselines.py` 只委托注册这些命令。|
|行为兼容验证|DONE|focused growth-tilt pytest、CLI help smoke、Ruff/compileall、docs/task consistency 和 `git diff --check` 通过；本轮未生成 cached-data dependent real report，因此未额外运行 `aits validate-data`。|

## Guardrails

- 不新增、删除或重命名任何外部 CLI command。
- 不改变 command 参数、默认路径、artifact path、report schema、status enum、
  safety fields、data quality gate 或 fail-closed 语义。
- 不改变 threshold、score band、promotion gate、position constraint、
  backtest acceptance rule、market-regime interpretation 或投资解释。
- 不写 production weights、active shadow weights、paper account state、broker order
  或 trading action。
- 如果验证发现 growth-tilt CLI 输出或 safety boundary 发生语义变化，停止并按
  no-silent-workaround 流程记录 blocker，不提交未验证重构。

## 进展记录

- 2026-06-26: 新增任务并进入 `IN_PROGRESS`。本轮维护目标是拆出
  growth-tilt CLI adapter 边界，降低 `research_simple_baselines.py` 继续膨胀的
  维护风险；预期无外部行为变化。
- 2026-06-26: 实现完成并转入 `DONE`。新增
  `cli_commands/research_growth_tilt.py`，原 `research_simple_baselines.py` 继续统一
  委托注册 growth-tilt commands；外部命令名、参数、artifact path、report schema、
  data quality gate、status/safety fields 和投资解释保持兼容。验证通过 focused
  xdist pytest、5 个 growth-tilt CLI help smoke、Ruff、compileall、docs/task
  consistency 和 `git diff --check`。
