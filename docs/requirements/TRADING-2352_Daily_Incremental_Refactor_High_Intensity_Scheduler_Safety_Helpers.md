# TRADING-2352 Daily Incremental Refactor: High-Intensity Scheduler Safety Helpers

最后更新：2026-07-05

## Status

DONE

## Context

本轮每日增量重构巡检基线为 `c1224b51a38036c03578da4687736649fa9dea07`，当前 HEAD 为 `1d603195d2147c523d6a70d27389dc35d4106561`。HEAD 是 TRADING-2350 manual-run dry-run 功能提交，不是合格重构提交。

基线之后 high-intensity risk-cap observe-only scheduler / manual-run 链路快速增长。TRADING-2347 disabled wiring、TRADING-2348 smoke dry-run、TRADING-2349 manual review gate 和 TRADING-2350 manual-run dry-run 均需要扫描 payload，防止真实 scheduler、event append、outcome binding、target weight、rebalance、paper-shadow、production 或 broker action 泄漏。当前这些扫描 helper 在相邻模块重复维护。

## Scope

- 新增内部 common helper，集中 unsafe field、real scheduler creation 和 forbidden action emission 的递归扫描逻辑。
- 让 TRADING-2347～2350 scheduler safety modules 复用该 helper。
- 保留各模块自己的 task id、status、readiness、report type、safety fields、forbidden field set 和 error type。
- 不实现 TRADING-2351 manual-run replay。
- 不改外部 CLI、artifact paths、report schema、data quality gate、threshold、score band、promotion gate、paper-shadow、production 或 broker behavior。

## Acceptance Criteria

- 相关 modules 的 generated payload keys、status/readiness、safety fields 和 Markdown output 保持兼容。
- Unsafe scheduler / target weight / rebalance / broker detection 仍 fail closed。
- Focused high-intensity scheduler tests 通过并行 pytest。
- Ruff、compileall、docs freshness、task-register consistency 和 `git diff --check` 通过。
- `docs/refactor_log.md` 记录本轮基线、评估范围、行为影响、验证命令和最终提交 SHA。

## Progress

- 2026-07-05: 新增任务，准备抽取内部 safety scanner helper。Production effect 固定 `none`；不写 production weights、active shadow weights，不触发 broker/order/trading action。
- 2026-07-05: 实现完成并归档 `DONE`。新增 `src/ai_trading_system/high_intensity_risk_cap_scheduler_common.py`，让 TRADING-2347 disabled wiring 与 TRADING-2348 smoke dry-run 复用 unsafe-field、real-scheduler 和 forbidden-action 扫描 helper；TRADING-2349 manual review gate 与 TRADING-2350 manual-run dry-run 继续通过 smoke dry-run wrapper 继承同一安全扫描行为。外部 CLI、artifact path、JSON/Markdown schema、status/readiness、安全字段、promotion / paper-shadow / production / broker 边界和投资解释保持不变。验证覆盖 Ruff、compileall、focused high-intensity scheduler parallel pytest 44 passed、docs freshness、task-register checks 和 `git diff --check`。本轮未运行 `aits validate-data`，因为不读取 cached market/macro data，不生成 technical features、scoring、backtest 或 daily report 输出。
