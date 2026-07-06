# TRADING-2399: Daily Incremental Refactor Dynamic Strategy JSON Helper Boundary

最后更新：2026-07-07

## Context

每日增量重构巡检从最近合格重构基线
`96855c7e7ce0b628c47b3aacb160bf6efec11341` 到当前 HEAD
`edda8d0703b919802fee801b5c9c158aa52d497b` 评估新增代码、配置、测试和文档。
增量范围集中在 TRADING-2389～2397 dynamic strategy research-only
报告生成链路，新增多个 builder module 与 owner/recombination decision
artifact。

这些模块继续遵守 no production / no broker / no order 边界，但重复维护
JSON block formatting 与 JSON document loading helper。现有
`src/ai_trading_system/dynamic_strategy_report_common.py` 已承载 JSON /
Markdown artifact writer，可作为同一报告族的低风险 helper 边界。

## Scope

本轮只整理 dynamic strategy research-only 报告模块的内部 helper 边界：

1. 在 `dynamic_strategy_report_common.py` 中集中 JSON block formatting 和
   JSON document loading helper。
2. 迁移 TRADING-2389～2397 增量新增模块中重复最集中的 owner/recombination
   document generation paths。
3. 保持外部 CLI、artifact path、JSON key、Markdown section、status enum、
   safety fields、data-quality disclosure 和 fail-closed semantics 不变。
4. 更新 `docs/refactor_log.md` 和任务登记；若本轮只整理内部 helper，不改变
   CLI、关键配置、cache schema、report output、data quality gate、scoring、
   backtest behavior、market-regime interpretation 或主要数据流，则
   `docs/system_flow.md` 不需要更新。

## Safety Boundary

- `production_effect=none`。
- 不写 production weights、active shadow weights、paper-shadow state 或 owner
  decision state。
- 不触发 broker、order、trading action、scheduler、event append 或 daily report。
- 不改变任何 threshold、score band、promotion gate、backtest acceptance rule、
  position constraint、data quality gate 或 investment interpretation。
- 本轮不读取 fresh cached market/macro data，不生成 technical features、
  scoring、backtest 或 daily report 输出；因此不要求额外运行 `aits validate-data`。

## Implementation Steps

1. 添加共享 helper，并保持既有 writer helper API 兼容。
2. 迁移选定 dynamic strategy modules 的 `_json_block` /
   `_load_json_document` 调用。
3. 运行 focused parallel pytest、CLI help smoke、Ruff、compileall、docs/task
   consistency 和 diff check。
4. 记录 `docs/refactor_log.md`，提交后回填本轮重构提交 SHA。

## Acceptance Criteria

- 受影响 dynamic strategy CLI help 仍可用。
- 受影响 focused tests 通过，证明原 report/build contract 未变化。
- Ruff、compileall、docs freshness、task-register consistency 和 `git diff --check`
  通过。
- `docs/refactor_log.md` 记录本轮基线、范围、变更文件、行为影响、数据/投资
  解释影响、验证结果和最终提交 SHA。
- 本任务完成后从 `docs/task_register.md` 移动到
  `docs/task_register_completed.md`。

## Open Questions

无。该切片仅整理内部 helper，不需要 owner 做投资解释或阈值政策决策。

## Progress

- 2026-07-07：每日增量重构巡检登记并进入 `IN_PROGRESS`；基线为
  `96855c7e7ce0b628c47b3aacb160bf6efec11341`，当前 HEAD
  `edda8d0703b919802fee801b5c9c158aa52d497b` 不是重构提交。
- 2026-07-07：实现完成并归档 `DONE`。新增共享 JSON helper，并迁移
  TRADING-2389～2397 dynamic strategy calibrated gate / component attribution /
  component recombination / recombination owner decision modules。验证已覆盖
  Ruff、compileall、focused parallel pytest 27 passed、9 个 CLI help smoke、
  docs freshness、docs/task focused pytest、terminal active-task 状态扫描和
  `git diff --check`；本轮不读取 fresh cached market/macro data，不生成
  technical features、scoring、backtest 或 daily report 输出，因此未额外运行
  `aits validate-data`。
