# TRADING-2425 Growth Tilt Engine Paper Shadow Dry-Run Wiring

## 状态

- 状态：`DONE`
- 优先级：`P0`
- task register：`TRADING-2425_GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING`
- owner route：`TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Schedule_Dry_Run`
- blocked route：`TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring_Gap_Remediation`
- 最后更新：2026-07-09

## 背景

TRADING-2424 已完成 Growth Tilt Engine paper-shadow enablement plan，真实 CLI status
为 `GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY`，且
enablement_plan_ready=true、enablement_gap_count=0、paper-shadow / schedule /
production / broker 全部 disabled。

TRADING-2425 只建立 paper-shadow dry-run wiring 证据，证明未来 paper-shadow
runtime 所需输入、输出、registry/catalog/docs route、manual review handoff 和
schedule hook disabled check 可以安全接线。它不是 runtime enablement，不得运行真实
paper-shadow daily job 或启用 schedule。

## 范围

- 新增 Growth Tilt Engine paper-shadow dry-run wiring builder。
- 新增 CLI：`aits research strategies growth-tilt-engine-paper-shadow-dry-run-wiring`。
- 读取 TRADING-2424 paper-shadow enablement plan artifact。
- 读取 TRADING-2423 paper-shadow preflight artifact。
- 读取 TRADING-2422 contract readiness snapshot。
- 读取 TRADING-2421 PIT gate readiness artifact。
- 读取 TRADING-2420 source traceability remediation artifacts。
- 读取 report registry、artifact catalog、system flow 和 research docs。
- 生成 paper-shadow dry-run wiring artifact。
- 生成 input/output contract map。
- 生成 dry-run runtime boundary manifest。
- 生成 schedule hook disabled verification。
- 生成 manual review handoff wiring plan。
- 生成 dry-run no-effect audit summary。
- 更新 research docs、report registry、artifact catalog、system flow、task register。
- 增加 focused tests。

## 非范围

- 不启用 paper-shadow。
- 不启用 paper-shadow schedule。
- 不运行真实 paper-shadow daily job。
- 不读取 fresh cached market data。
- 不运行 backtest、scoring 或 daily report。
- 不生成新 signal。
- 不生成 trading advice。
- 不修改实际组合权重。
- 不启用 production。
- 不触发 broker action。
- 不绕过 manual review。

## 实施步骤

1. 登记任务和需求文档。
2. 新增 research-quality builder，fail-closed 检查 2424/2423/2422/2421/2420
   evidence、input/output contract map、manual review handoff route、disabled schedule
   hook、registry/catalog/system-flow/doc references 和 safety boundaries。
3. 新增 wrapper，读取 prior artifacts/docs 并输出 JSON / Markdown artifacts。
4. 新增 CLI wiring 和 deterministic summary output。
5. 新增 focused tests 覆盖 READY、blocked、missing contract map、安全边界、CLI 和
   registry/catalog/flow。
6. 运行真实 CLI 生成 artifacts 和 research docs。
7. 更新 report registry、artifact catalog、system flow，并完成 task archive closeout。
8. 运行验证、commit 并 push。

## 验收标准

- CLI 可真实运行并输出 deterministic status。
- READY 时 status=`GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_READY`。
- blocked 时 status=`GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_BLOCKED_BY_WIRING_GAPS`。
- READY artifact 中 dry_run_wiring_ready=true、dry_run_wiring_gap_count=0。
- input_contract_map_ready=true、output_artifact_contract_map_ready=true。
- manual_review_handoff_wired=true、schedule_hook_verified_disabled=true、
  no_effect_audit_ready=true。
- `paper_shadow_enabled=false`、`paper_shadow_schedule_enabled=false`、
  `production_enabled=false`、`broker_enabled=false`、`automatic_execution_allowed=false`。
- `generated_signal=false`、`generated_trading_advice=false`、`backtest_run=false`、
  `scoring_run=false`、`daily_report_run=false`。
- READY next route=`TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Schedule_Dry_Run`。
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
  `GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_READY`，
  dry_run_wiring_ready=true，dry_run_wiring_gap_count=0，
  input_contract_map_ready=true，output_artifact_contract_map_ready=true，
  manual_review_handoff_wired=true，schedule_hook_verified_disabled=true，
  no_effect_audit_ready=true，paper_shadow_enabled=false，
  paper_shadow_schedule_enabled=false，production_enabled=false，broker_enabled=false，
  next route=`TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Schedule_Dry_Run`。
