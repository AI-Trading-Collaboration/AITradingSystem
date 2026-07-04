# ENG-VAL-007 Controlled Strategy Candidate Batch Fixture Reuse

最后更新：2026-07-05

## 背景

ENG-VAL-006 后，`contract-validation`
`outputs/validation_runtime/contract-validation_20260704T193021Z/test_runtime_summary.json`
显示 tier elapsed 已降到 `93.19s`，但 slow durations 中
`tests/test_controlled_strategy_candidate_batch.py` 仍多次出现：

- `test_gbdt_action_utility_schema`
- `test_controlled_strategy_batch_review_schema`
- `test_kill_pause_pivot_enum`
- `test_no_promotion_from_controlled_review`
- `test_next_batch_recommendation_present`
- `test_all_candidates_have_decision`
- `test_gbdt_no_future_features`

该文件重复执行 price cache 写入、candidate builder 和 batch review 前置构造。

## 目标

将 `tests/test_controlled_strategy_candidate_batch.py` 中重复的 candidate input
chains 提升为 module-scoped pytest fixtures。共享 fixtures 只提供只读前置 artifact
paths 或 payloads；每个测试仍保留对应 schema / safety / payload assertions。

## 范围

包含：

- 为 regret state machine、simple selector、GBDT baseline、GBDT pivot、candidate
  batch review 等重复链路建立 module-scoped fixtures；
- 保留现有 direct API / builder coverage；
- 用 focused pytest 对比优化前后运行时间；
- 运行 Ruff、compileall、docs/task-register gates、`contract-validation` 和
  `git diff --check`。

不包含：

- 修改 `src/ai_trading_system/controlled_strategy_batch.py`；
- 修改 `test_controlled_strategy_batch.py` CLI smoke 语义；
- 减少 `fast-unit` 或 `contract-validation` tier 覆盖；
- 修改 payload schema、report registry、artifact catalog、pass/fail 判定；
- 修改默认 validation runner `-n 16 --dist loadfile`；
- 读取或刷新 fresh market data；
- 修改 paper-shadow、production 或 broker/order 边界。

## 验收标准

- Focused baseline 与优化后 focused pytest 均有记录；
- `tests/test_controlled_strategy_candidate_batch.py` 通过 parallel pytest；
- `contract-validation` 通过并写入 runtime artifact；
- 文档 freshness、documentation contract、task-register consistency run/validate 通过；
- `git diff --check` 通过；
- 完成说明明确 `aits validate-data` 不适用原因。

## 状态记录

- 2026-07-05：根据 ENG-VAL-006 后的 slow-duration evidence 新增并进入
  `IN_PROGRESS`。当前优化候选是
  `tests/test_controlled_strategy_candidate_batch.py` 的 repeated candidate /
  batch-review input chains。
- 2026-07-05：实现完成并归档 `DONE`。`tests/test_controlled_strategy_candidate_batch.py`
  改用 module-scoped 只读 payload / artifact fixtures，保留原有 schema、
  safety 和 payload field assertions。Focused pytest 从 `23 passed in 82.30s`
  降到 `23 passed in 20.20s`；`contract-validation` 通过 `197 passed`，
  runtime artifact=`outputs/validation_runtime/contract-validation_20260704T194035Z/test_runtime_summary.json`，
  tier elapsed 从 `93.19s` 小幅降到 `91.47s`。本次说明 candidate-batch
  repeated construction 已不再是完整 tier critical path；后续瓶颈转为
  tail-risk independent setup、current subscription CLI smoke、controlled strategy
  batch CLI smoke、value-surface repeated construction 和若干单个 CLI smoke。
