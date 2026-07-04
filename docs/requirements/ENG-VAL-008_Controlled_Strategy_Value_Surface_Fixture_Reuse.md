# ENG-VAL-008 Controlled Strategy Value Surface Fixture Reuse

最后更新：2026-07-05

## 背景

ENG-VAL-007 后，`contract-validation`
`outputs/validation_runtime/contract-validation_20260704T194035Z/test_runtime_summary.json`
仍约 `91.47s`。Focused baseline 显示
`tests/test_controlled_strategy_value_surface.py` 为 `21 passed in 81.14s`，
且慢项集中在 repeated value-surface / next-stage / direction-review input
chains。

## 目标

将 `tests/test_controlled_strategy_value_surface.py` 中重复的 value-surface
前置 artifact chains 提升为 module-scoped pytest fixtures。共享 fixtures 只提供
只读 payloads 或 artifact paths；测试仍保留 schema / safety / payload field
assertions。

## 范围

包含：

- 为 value-surface prototype、controlled expansion、next-stage inputs 和
  direction-review inputs 建立 module-scoped fixtures；
- 保留 control-failure 等变体测试的独立 builder 执行；
- 用 focused pytest 对比优化前后运行时间；
- 运行 Ruff、compileall、docs/task-register gates、`contract-validation` 和
  `git diff --check`。

不包含：

- 修改 `src/ai_trading_system/controlled_strategy_batch.py`；
- 修改 CLI smoke 语义；
- 减少 `fast-unit` 或 `contract-validation` tier 覆盖；
- 修改 payload schema、report registry、artifact catalog、pass/fail 判定；
- 修改默认 validation runner `-n 16 --dist loadfile`；
- 读取或刷新 fresh market data；
- 修改 paper-shadow、production 或 broker/order 边界。

## 验收标准

- Focused baseline 与优化后 focused pytest 均有记录；
- `tests/test_controlled_strategy_value_surface.py` 通过 parallel pytest；
- `contract-validation` 通过并写入 runtime artifact；
- 文档 freshness、documentation contract、task-register consistency run/validate 通过；
- `git diff --check` 通过；
- 完成说明明确 `aits validate-data` 不适用原因。

## 状态记录

- 2026-07-05：根据 ENG-VAL-007 后的 focused runtime evidence 新增并进入
  `IN_PROGRESS`。当前优化候选是
  `tests/test_controlled_strategy_value_surface.py` 的 repeated value-surface /
  next-stage / direction-review input chains。
- 2026-07-05：实现完成并归档 `DONE`。`tests/test_controlled_strategy_value_surface.py`
  改用 module-scoped 只读 payload / artifact fixtures，保留 control-failure
  变体独立 builder 执行以及原有 schema、safety 和 payload field assertions。
  Focused pytest 从 `21 passed in 81.14s` 降到 `21 passed in 25.69s`；
  `contract-validation` 通过 `197 passed`，runtime
  artifact=`outputs/validation_runtime/contract-validation_20260704T194916Z/test_runtime_summary.json`，
  tier elapsed 从 `91.47s` 降到 `35.70s`。当前剩余 top slow 项主要是
  tail-risk module setup 和若干真实 CLI smoke；继续压缩低风险收益已明显变弱。
