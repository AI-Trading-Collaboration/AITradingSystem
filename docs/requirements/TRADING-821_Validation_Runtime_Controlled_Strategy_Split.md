# TRADING-821: Validation Runtime Controlled Strategy Split
最后更新：2026-06-23

## Context

TRADING-816～820 完成后，验证耗时主要集中在 controlled strategy pytest：

- `tests/test_controlled_strategy_batch.py` 单文件并行 pytest：约 9 分钟；
- `fast-unit`：约 9 分钟，其中包含同一大文件；
- `contract-validation`：约 9 分钟，其中也包含同一大文件；
- `report-validation`：约 27 秒。

项目当前默认使用 `pytest-xdist --dist loadfile`。该策略能减少跨文件共享状态风险，
但当 70+ 个测试集中在单个文件时，16 个 worker 不能有效分摊负载。

## Goal

在不降低验证覆盖、不改变 production / research 行为的前提下，把 controlled strategy batch
测试拆成多个按主题划分的文件，使现有 `--dist loadfile` 能并行调度。

## Safety Boundary

本任务只允许改变测试组织、validation tier path 列表和测试辅助模块：

- 不改变 `src/ai_trading_system/controlled_strategy_batch.py` 的业务逻辑；
- 不改变 CLI 行为、artifact schema、report registry 语义或 investment conclusion；
- 不改变 validation tier safety boundary；
- 不把 `--dist loadfile` 改成更激进的 cross-test distribution；
- 不减少 `fast-unit` 或 `contract-validation` 的 controlled strategy 覆盖。

## Implementation Plan

1. 将公共测试 helper、fixture writer、candidate chain builder 抽到
   `tests/controlled_strategy_batch_helpers.py`。
2. 将原 `tests/test_controlled_strategy_batch.py` 拆为多个主题文件：
   - value-surface / utility / forward evidence；
   - regime / horizon / tail-risk policy family；
   - batch-1 candidate modules；
   - CLI smoke / registry / catalog / system-flow contract。
3. 更新 `scripts/run_validation_tier.py`，让 `fast-unit` 和 `contract-validation` 显式包含拆分后的
   controlled strategy test files。
4. 更新相关测试中的 validation tier membership assertion。
5. 运行 focused parallel pytest、`fast-unit`、`contract-validation`，并记录 runtime 对比。

## Acceptance Criteria

- 拆分后的 focused controlled strategy pytest 全部通过。
- `fast-unit` 和 `contract-validation` 仍包含全部拆分文件。
- `test_controlled_strategy_batch_cli_smoke` 仍在 tier 中执行。
- `--dist loadfile` 默认不变。
- Runtime summary 能显示 controlled strategy tests 被多文件分发。

## Progress Notes

- 2026-06-22：新增任务并进入 `IN_PROGRESS`；当前优先执行测试拆分，不改变 validation
  tier distribution policy。
- 2026-06-22：实现完成并转入 `VALIDATING`。拆分结果：
  `tests/controlled_strategy_batch_helpers.py` 承载公共 fixture/helper；
  `tests/test_controlled_strategy_value_surface.py` 覆盖 value surface / utility / forward evidence；
  `tests/test_controlled_strategy_regime_horizon.py` 覆盖 regime-conditioned / horizon selector；
  `tests/test_controlled_strategy_tail_risk_policy.py` 覆盖 tail-risk policy family；
  `tests/test_controlled_strategy_candidate_batch.py` 覆盖 batch-1 candidate modules；
  `tests/test_controlled_strategy_batch.py` 保留 CLI smoke、validation tier membership 和
  registry/catalog/system-flow contract。
- 2026-06-22：验证通过。Focused controlled strategy pytest 73 passed，用时 132.52s；
  拆分前同等 suite 为 552.17s，缩短约 76.0%。`fast-unit` 178 passed，用时 136.55s，
  runtime artifact:
  `outputs/validation_runtime/fast-unit_20260621T162455Z/test_runtime_summary.json`；
  拆分前为 549.89s，缩短约 75.2%。`contract-validation` 177 passed，用时 138.91s，
  runtime artifact:
  `outputs/validation_runtime/contract-validation_20260621T162717Z/test_runtime_summary.json`；
  拆分前为 559.78s，缩短约 75.2%。Ruff、Black、compileall 通过，`--dist loadfile`
  默认保持不变。
