# TRADING-2391: Daily Incremental Refactor Dynamic Strategy Report Writer Boundary

最后更新：2026-07-06

## Status

DONE

## Context

本轮每日增量重构巡检基线为
`2a4b9f222fb55e554e4efce8ff828a49c7f2621d`，当前 HEAD 为
`8a668cd969013425d0724d6aed8217d1f27b6188`。HEAD 是 TRADING-2388
research filter threshold methodology review 功能提交，不是合格重构提交。

基线之后 dynamic strategy research-only 链路快速新增 TRADING-2364～2388
报告模块。多个模块在 `_write_outputs` 中重复维护 JSON / Markdown artifact
writer；TRADING-2388 作为最新 threshold methodology review 模块，已经有 5 个
JSON 和 5 个 Markdown 输出，后续 TRADING-2389 / 2390 若继续复制 writer，会提高
artifact path、encoding、sort order 和 parent-directory 行为分叉风险。

## Scope

- 新增内部 dynamic strategy report writer helper，集中 JSON / Markdown artifact
  写入行为。
- 先迁移 TRADING-2388 threshold methodology review 的 writer 调用。
- 保留 TRADING-2388 自己的 task id、report type、schema、status、安全字段、source
  validation、threshold inventory、gate taxonomy、candidate matrix 和 route 内容。
- 不修改外部 CLI、artifact path、JSON key、Markdown 文案、report registry、artifact
  catalog、system flow、threshold、score band、promotion gate、data quality gate、
  backtest acceptance、market-regime interpretation、paper-shadow、production 或 broker
  behavior。

## Acceptance Criteria

- TRADING-2388 focused tests 继续通过，证明 builder、CLI、registry/docs contract 和
  safety fields 保持兼容。
- 新 helper 有 focused unit coverage，覆盖 parent directory creation 和 JSON / Markdown
  writer behavior。
- Ruff、compileall、docs freshness、task-register consistency、terminal active-task scan 和
  `git diff --check` 通过。
- `docs/refactor_log.md` 记录本轮基线、评估范围、变更文件、行为影响、验证命令和最终提交
  SHA。

## Progress

- 2026-07-06: 新增任务，准备抽取 dynamic strategy report writer helper。Production
  effect 固定 `none`；不写 production weights、active shadow weights，不触发 broker /
  order / trading action；本轮预计不运行 `aits validate-data`，因为目标切片只整理
  artifact writer，不读取 cached market / macro data，不生成 technical features、scoring、
  backtest 或 daily report 输出。
- 2026-07-06: 实现完成并归档 `DONE`。新增
  `src/ai_trading_system/dynamic_strategy_report_common.py`，集中 JSON / Markdown artifact
  writer；TRADING-2388 threshold methodology review 复用该 helper。外部 CLI、artifact
  path、JSON key、Markdown 文案、report registry、artifact catalog、system flow、阈值、
  score band、promotion gate、data quality gate、backtest、market-regime interpretation、
  paper-shadow、production 和 broker 边界保持不变。验证覆盖 Ruff、compileall、focused
  parallel pytest 5 passed、CLI help smoke、docs freshness 574 docs PASS、docs/task focused
  pytest 11 passed 和 `git diff --check` PASS（仅 CRLF normalization warning）。本轮未运行
  `aits validate-data`，因为不读取 cached market/macro data，不生成 technical features、
  scoring、backtest 或 daily report 输出。
