# TRADING-2424 Growth Tilt Engine Paper Shadow Enablement Plan

## 状态

- 状态：`DONE`
- 优先级：`P0`
- task register：`TRADING-2424_GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN`
- owner route：`TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring`
- blocked route：`TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Enablement_Gap_Remediation`
- 最后更新：2026-07-09

## 背景

TRADING-2423 已完成 Growth Tilt Engine paper-shadow preflight，真实 CLI status 为
`GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT_READY`，且 `pit_gate_ready=true`、
`contract_ready=true`、`contract_gap_count=0`、remaining PIT blockers=[]。

TRADING-2424 只把该 READY 状态转换成可审计的 paper-shadow enablement plan，证明
后续可以进入 dry-run wiring 设计。它不是 runtime enablement，不得启动
paper-shadow、schedule、production 或 broker/order。

## 范围

- 新增 Growth Tilt Engine paper-shadow enablement plan builder。
- 新增 CLI：`aits research strategies growth-tilt-engine-paper-shadow-enablement-plan`。
- 读取 TRADING-2423 paper-shadow preflight artifact。
- 读取 TRADING-2422 contract readiness snapshot。
- 读取 TRADING-2421 PIT gate readiness artifact。
- 读取 TRADING-2420 source traceability remediation artifacts。
- 读取 report registry、artifact catalog、system flow 和 research docs。
- 生成 enablement plan artifact。
- 生成 runtime boundary checklist。
- 生成 schedule boundary plan。
- 生成 manual review checklist。
- 生成 rollback / stop condition summary。
- 生成 TRADING-2425 dry-run wiring route。
- 更新 research docs、report registry、artifact catalog、system flow、task register。
- 增加 focused tests。

## 非范围

- 不启用 paper-shadow。
- 不启用 paper-shadow schedule。
- 不运行 paper-shadow daily job。
- 不生成新 signal。
- 不生成 trading advice。
- 不运行 backtest、scoring 或 daily report。
- 不修改实际组合权重。
- 不启用 production。
- 不触发 broker action。
- 不跳过 manual review。

## 实施步骤

1. 登记任务和需求文档。
2. 新增 research-quality builder，fail-closed 检查 2423/2422/2421/2420 evidence、
   registry/catalog/system-flow/doc references 和 safety boundaries。
3. 新增 wrapper，读取 prior artifacts/docs 并输出 JSON / Markdown artifacts。
4. 新增 CLI wiring 和 deterministic summary output。
5. 新增 focused tests 覆盖 READY、blocked、safety boundary、CLI 和 registry/catalog/flow。
6. 运行真实 CLI 生成 artifacts 和 research docs。
7. 更新 report registry、artifact catalog、system flow，并完成 task archive closeout。
8. 运行验证、commit 并 push。

## 验收标准

- CLI 可真实运行并输出 deterministic status。
- READY 时 status=`GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY`。
- blocked 时 status=`GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_BLOCKED_BY_PREFLIGHT_OR_CONTRACT_GAPS`。
- READY artifact 中 `enablement_plan_ready=true`、`enablement_gap_count=0`。
- `paper_shadow_enabled=false`、`paper_shadow_schedule_enabled=false`、
  `production_enabled=false`、`broker_enabled=false`。
- `generated_signal=false`、`generated_trading_advice=false`、`backtest_run=false`、
  `scoring_run=false`、`daily_report_run=false`。
- `manual_review_required=true`、`automatic_execution_allowed=false`。
- READY next route=`TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring`。
- report registry / artifact catalog / system flow / task register 一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、
  task-register consistency、contract-validation 和 `git diff --check` 通过。

## Data Quality Gate

本任务不运行 `aits validate-data`，前提是实现只读取 prior artifacts / registry /
catalog / docs / system_flow，不读取 fresh cached market data，不运行 backtest/scoring/
daily report，也不生成新 signal 或交易建议。

## 进展记录

- 2026-07-09：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI status=
  `GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY`，enablement_plan_ready=true，
  enablement_gap_count=0，paper_shadow_enabled=false，
  paper_shadow_schedule_enabled=false，production_enabled=false，broker_enabled=false，
  next route=`TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring`。
