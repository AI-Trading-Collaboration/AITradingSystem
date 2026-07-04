# ENG-VAL-005 Tail-Risk Test Fixture Reuse
最后更新：2026-07-05

## 背景

ENG-VAL-004 后，`contract-validation` runtime artifact 显示 docs contract 已退出 top20，
当前慢项集中在 tail-risk / controlled-strategy 测试族群：

- `tests/test_tail_risk_independent_validation_governance.py::test_tail_risk_independent_forward_outcome_contract`
  setup `23.26s`；
- `tests/test_tail_risk_fallback_falsification_audit.py` 多个测试约 `14.72s` 到 `19.15s`；
- `tests/test_controlled_strategy_tail_risk_policy.py` 多个测试约 `14.68s` 到 `16.45s`；
- 这些测试反复调用 `controlled_strategy_batch_helpers.py` 中的 tail-risk 输入链路，
  多次重建相同 price cache、value-surface、horizon selector、tail-risk policy 和
  review-board 前置 artifacts。

## 目标

在不降低测试覆盖、不改变 production/report builder 行为、不修改 validation runner 默认策略的
前提下，降低 tail-risk 测试群的重复 fixture 构造成本：

1. 把重复的 tail-risk 输入链路提升为 module-scoped pytest fixtures；
2. 每个测试仍运行自己要验证的 builder，并写入测试自己的 `tmp_path` 输出目录；
3. 共享 fixture 只提供只读前置 artifact paths，不共享被测试 builder 的输出；
4. 保持 direct builder coverage、assertions、safety checks 和 contract-validation tier path 不变。

## 安全边界

- 不修改 `src/ai_trading_system/controlled_strategy_batch.py` 或任何投资逻辑；
- 不修改 report registry、artifact catalog、data quality gate、cached data 或 runtime artifacts；
- 不改变 `scripts/run_validation_tier.py` 默认 `DEFAULT_DIST=loadfile` / `DEFAULT_WORKERS=16`；
- 不减少 tail-risk builder 测试数量、assertions 或 safety checks；
- 不生成 paper-shadow、production、official target weight、broker/order 或 runtime mutation。

## 实施步骤

|步骤|状态|验收标准|
|---|---|---|
|登记任务与需求|DONE|`docs/task_register.md` 与本需求文档记录范围、边界和验收标准|
|复用 tail-risk 前置输入 fixture|DONE|fallback falsification 与 tail-risk policy 测试改用 module-scoped 只读输入 paths|
|验证 runtime 收益|DONE|focused pytest、Ruff、compileall、contract-validation runtime artifact 和 docs/task gates 通过|
|归档|DONE|任务移动到 completed，并记录最终 runtime 对比|

## 进展记录

- 2026-07-05：根据 ENG-VAL-004 后的 slow-duration evidence 新增并进入 `IN_PROGRESS`。
  本任务只优化测试前置 artifact 构造组织，不改变 tail-risk builder 行为、contract 覆盖、
  validation runner 默认策略或生产边界。
- 2026-07-05：实现完成并归档 `DONE`。`tests/test_tail_risk_fallback_falsification_audit.py`
  与 `tests/test_controlled_strategy_tail_risk_policy.py` 改为 module-scoped 只读输入
  fixtures；每条测试仍执行自身目标 builder，并写入独立 `tmp_path`。Focused 两文件
  pytest 从 `20 passed in 160.18s` 降到 `20 passed in 19.43s`；慢项从 20 个
  `11.34s` 到 `18.01s` 的 call duration，收敛为 `12.26s` / `10.54s` 的一次性
  module setup。`contract-validation` 通过 `197 passed`，runtime artifact=
  `outputs/validation_runtime/contract-validation_20260704T185549Z/test_runtime_summary.json`，
  tier elapsed 从 `181.10s` 降到 `165.93s`，top20 slow duration total 从
  `340.96s` 降到 `260.25s`。当前最慢项转为 tail-risk independent governance
  setup、controlled strategy batch CLI smoke、current subscription CLI smoke，以及
  controlled strategy regime-horizon / candidate-batch 重复构造。
