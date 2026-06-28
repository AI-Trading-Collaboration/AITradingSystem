# TRADING-2270 First-Layer Current State And Failure Taxonomy

最后更新：2026-06-28

## 背景

附件要求在继续 proxy audit / objective redesign / challenger experiments 之前，
先把当前 first-layer baseline 的失败形态落成可审计现状报告。当前结论来自
`first_layer_composer_v2`、channel-specific v3、Norgate trial partial evidence 和
post-2085 reopen gate：工程链路可运行，但 first-layer trend value 尚未证明，不能
恢复 production、paper-shadow、broker 或 reopen gate。

## 范围

- 生成 `first_layer_current_state_report.md`。
- 生成 `first_layer_failure_taxonomy.json`。
- 生成 `benchmark_consistency_report.json`。
- 生成 `regime_slice_summary.md`。
- 覆盖 false risk-on、false risk-off、late risk-off、late risk-on、regime flip 和
  benchmark consistency。
- benchmark 至少覆盖 QQQ / SPY / SMH；IWM / RSP 仅在本地 cache 有数据时纳入。
- regime slices 覆盖 2022 bear/rate shock、2023 recovery、2024 AI concentration、
  2025-2026 trial-like window。

## 非目标

- 不训练新模型。
- 不购买 Norgate Platinum。
- 不把 `validation_ready` 等同 promotion。
- 不打开 first-layer reopen、paper-shadow、production 或 broker gate。
- 不输出 target weights、trade action、recommended allocation 或 TQQQ allocation。

## 实施步骤

1. 登记任务和 pilot taxonomy policy，说明阈值只用于失败归因，不是投资门槛。
2. 新增 current-state runner，先复用 cached-data validation code path，再读取
   `first_layer_composer_v2_predictions.csv` 与 benchmark adjusted-close cache。
3. 输出 failure taxonomy、benchmark consistency、regime slice summary 和中文现状报告。
4. 更新 report registry、artifact catalog、system flow 和 task register。
5. 运行 focused parallel pytest、Ruff、compileall、docs/register checks 和真实 CLI。

## 验收标准

- 四个附件要求 artifacts 均可由 CLI 重新生成。
- artifacts 披露 `market_regime=ai_after_chatgpt`、requested date range、actual signal range
  和 data quality status。
- QQQ / SPY / SMH benchmark consistency 可审计；缺失的 IWM / RSP 必须显式标记
  `data_available=false`。
- 2022 slice 如果没有 baseline signal coverage，必须显式记录，不得把 2023+ 结果倒推
  成 2022 stress validation。
- 所有 gate fields 固定 false / `none` / `BLOCKED`。

## 进展

- 2026-06-28：新增需求文档并进入 `IN_PROGRESS`。本批只做现状和失败 taxonomy，不进入
  TRADING-2271～2273，也不改变 Norgate purchase decision。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增
  `aits research trends first-layer-current-state`、pilot policy、failure taxonomy、
  benchmark consistency、current-state report、regime slice summary、summary YAML 和
  focused tests。真实 run data_quality_status=`PASS_WITH_WARNINGS`，actual signal range
  `2023-02-22`～`2026-03-27`，raw filtered signal rows=2205，deduplicated dated signals=777，
  duplicate rows removed=1428；QQQ/SPY/SMH available，IWM/RSP unavailable，2022 slice
  `NO_SIGNAL_COVERAGE`。所有 gates 保持 false/none/BLOCKED。
- 2026-06-28：验证通过 Ruff、compileall、focused parallel pytest（2 passed）、docs/report/
  task-register/heuristic governance focused parallel pytest（47 passed）、
  `python -m ai_trading_system.cli docs validate-freshness`、`git diff --check` 和
  `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
  （193 passed）；runtime artifact=
  `outputs/validation_runtime/contract-validation_20260628T120609Z/test_runtime_summary.json`。
