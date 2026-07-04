# ENG-VAL-006 Controlled Strategy Regime/Horizon Fixture Reuse

最后更新：2026-07-05

## 背景

ENG-VAL-005 后，`contract-validation` 最新 runtime artifact
`outputs/validation_runtime/contract-validation_20260704T191536Z/test_runtime_summary.json`
显示 tier 仍通过，但 slow durations 已转向 controlled strategy regime/horizon 和
candidate-batch 重复构造：

- `tests/test_controlled_strategy_regime_horizon.py` 中多个测试重复执行同一条
  value-surface / regime-conditioning / horizon-selector input chain；
- top20 slow durations 中该文件占据多项 9s-12s call time；
- 当前任务目标是继续压缩 validation runtime，而不是改变任何 investment-facing
  builder、报告 schema 或 gate 判定。

## 目标

将 `tests/test_controlled_strategy_regime_horizon.py` 中重复的前置 artifact chain
提升为 module-scoped pytest fixtures。共享 fixtures 只提供只读前置 artifact
paths；每个测试仍执行自身目标 builder，仍写入自己的 `tmp_path` 输出目录。

## 范围

包含：

- 为 regime-conditioning、value-surface-v2、horizon-selector inputs 建立
  module-scoped fixtures；
- 保留 direct builder assertions、payload schema checks 和 safety assertions；
- 用 focused pytest 对比优化前后运行时间；
- 运行 Ruff、compileall、docs/task-register gates、`contract-validation` 和
  `git diff --check`。

不包含：

- 修改 `src/ai_trading_system/controlled_strategy_batch.py` production builders；
- 减少 `fast-unit` 或 `contract-validation` tier 覆盖；
- 修改 payload schema、report registry、artifact catalog、pass/fail 判定；
- 修改默认 validation runner `-n 16 --dist loadfile`；
- 读取或刷新 fresh market data；
- 修改 paper-shadow、production 或 broker/order 边界。

## 验收标准

- Focused baseline 与优化后 focused pytest 均有记录；
- `tests/test_controlled_strategy_regime_horizon.py` 通过 parallel pytest；
- `contract-validation` 通过并写入 runtime artifact；
- 文档 freshness、documentation contract、task-register consistency run/validate 通过；
- `git diff --check` 通过；
- 完成说明明确 `aits validate-data` 不适用原因。

## 状态记录

- 2026-07-05：根据 `contract-validation_20260704T191536Z` slow-duration evidence
  新增并进入 `IN_PROGRESS`。当前优化候选是
  `tests/test_controlled_strategy_regime_horizon.py` 的 repeated input chain。
- 2026-07-05：实现完成并归档 `DONE`。`tests/test_controlled_strategy_regime_horizon.py`
  改用 module-scoped 只读前置 input fixtures，每个测试仍执行自身目标 builder
  并写入独立 `tmp_path`。Focused pytest 从 `15 passed in 128.84s` 降到
  `15 passed in 18.86s`；`contract-validation` 通过 `197 passed`，
  runtime artifact=`outputs/validation_runtime/contract-validation_20260704T193021Z/test_runtime_summary.json`，
  tier elapsed 从 `164.33s` 降到 `93.19s`，top20 slow duration total
  从 `258.61s` 降到 `185.56s`。后续瓶颈转为 tail-risk independent setup、
  current subscription CLI smoke、controlled strategy batch CLI smoke、
  controlled strategy candidate-batch repeated construction 和 value-surface
  repeated construction。
