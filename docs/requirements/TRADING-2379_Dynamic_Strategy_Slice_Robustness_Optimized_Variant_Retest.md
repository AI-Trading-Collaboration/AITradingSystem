# TRADING-2379 Dynamic Strategy Slice-Robustness Optimized Variant Retest

最后更新：2026-07-05

## 背景

TRADING-2378 已完成 slice robustness and return-gap optimization plan，真实状态为
`DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY`。
2378 将下一步 route 固定为
`TRADING-2379_Dynamic_Strategy_Slice_Robustness_Optimized_Variant_Retest`。

TRADING-2376 targeted retest 显示
`dynamic_regime_overlay_v0_4_lower_turnover` 在 realistic / conservative / harsh
cost stress 下仍保持正 gap，但 time-slice、regime-slice 和相对
`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` 的 return gap 仍未满足升级条件。

## 范围

本任务新增
`aits research strategies dynamic-strategy-slice-robustness-optimized-variant-retest`，
读取 TRADING-2365 / 2366 / 2375 / 2376 / 2378 artifacts，并在通过
`aits validate-data --as-of 2026-07-05` 后对 2378 规划 variants 执行实际 retest。

必须覆盖：

- base reference：`dynamic_regime_overlay_v0_4_lower_turnover`
- `dynamic_regime_overlay_v0_4_reentry_repair_v1`
- `dynamic_regime_overlay_v0_4_upside_capture_guarded_v1`
- `dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1`
- `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
- `dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1`
- comparison：`static_baseline` 与
  `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`

## 允许与禁止

允许：

- 读取既有 market / rates cache、strategy config 和 research artifacts。
- 运行 data quality gate。
- 运行 research backtest / variant retest。
- 计算 time/regime slice、cost stress、turnover / constraint stress 和 ranking。
- 生成 research-only JSON / Markdown 报告。

禁止：

- 启用 scheduler、创建 scheduled task 或生成 daily report。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 进入 production、调用 broker API 或生成 order。
- 把 monthly rebalance 作为 primary decision 或 primary ranking 依据。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source registration and validation|DONE|登记任务；fail-closed 校验 2365 / 2366 / 2375 / 2376 / 2378 source status、2378 route、primary candidate、ranking top、primary cadence 和 safety fields。|
|2. Data quality gate|DONE|执行并记录 `aits validate-data --as-of 2026-07-05` / 同源 validation code path；gate 不通过时停止 retest。|
|3. Variant retest engine|DONE|对 base 和 5 个 planned variants 生成可审计 target weights，并在 `valid_until_window` 主口径下运行实际 retest。|
|4. Slice / cost / constraint matrices|DONE|输出 time-slice、regime-slice、cost-stress、turnover / constraint stress 结果；monthly rebalance 仅可作为 legacy reference，不能主导 ranking。|
|5. Ranking and decision update|DONE|生成 optimized variant ranking、best variant、decision enum、decision reasons 和 TRADING-2380 route。|
|6. Registry, docs and validation|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc、research docs、focused tests，并完成 full validation / commit / push。|

## Heuristic Governance

2379 的 decision thresholds 属于 research-only pilot policy。实现必须把阈值写成命名常量或
policy payload，并在输出中暴露 policy id / rationale / review condition。该任务不能把阈值
升级为 production promotion gate，也不能批准 paper-shadow。

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_slice_robustness_optimized_variant_retest/variant_retest_result.json
outputs/research_strategies/dynamic_strategy_slice_robustness_optimized_variant_retest/optimized_variant_ranking.json
outputs/research_strategies/dynamic_strategy_slice_robustness_optimized_variant_retest/time_regime_slice_matrix.json
outputs/research_strategies/dynamic_strategy_slice_robustness_optimized_variant_retest/decision_update.json
docs/research/dynamic_strategy_slice_robustness_optimized_variant_retest.md
docs/research/dynamic_strategy_optimized_variant_ranking.md
docs/research/dynamic_strategy_optimized_variant_slice_matrix.md
docs/research/dynamic_strategy_2380_route.md
```

## 验收标准

- CLI 返回 `DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY`。
- 输出包含 variants_tested、optimized_variant_ranking、time/regime slice matrix、
  cost stress result、turnover constraint result、best variant、best variant decision 和
  TRADING-2380 route。
- `primary_execution_cadence=valid_until_window`；monthly rebalance 不作为 primary decision。
- 输出必须明确是否缩小 return gap、是否改善 time/regime robustness、是否保留 lower-turnover
  profile、是否穿越 realistic / conservative cost，以及是否可进入 research-only observation。
- scheduler、event append、outcome binding、paper-shadow、paper trade、shadow position、
  production、broker、daily report 全部保持 false / none。

## 进展记录

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 strategy research
  optimized variant retest，不是 observation approval、paper-shadow approval、scheduler
  enablement、production 或 broker readiness。
- 2026-07-05: 实现完成并归档 `DONE`。新增 optimized variant retest CLI、
  builder、ranking、slice matrix、decision update、research docs、registry、catalog、
  system flow 和 focused tests。真实 run status 为
  `DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY`，
  data quality 为 `PASS_WITH_WARNINGS` / errors=0 / warnings=2；best variant 为
  `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`，best decision 为
  `CONTINUE_OPTIMIZATION`，next route 为
  `TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_Observation_Decision`。
  本任务没有批准 observation、paper-shadow、scheduler、daily report、production 或 broker。
- 2026-07-05: closeout 验证通过 `aits validate-data --as-of 2026-07-05`
  PASS_WITH_WARNINGS / errors=0 / warnings=2、full Ruff、`compileall -q src tests`、
  focused parallel pytest 3 passed、真实 CLI run、docs freshness 564 docs PASS、
  documentation contract 1276 reports PASS、task-register consistency run/validate
  PASS、contract-validation 197 passed（runtime artifact：
  `outputs/validation_runtime/contract-validation_20260705T143940Z/test_runtime_summary.json`）
  和 `git diff --check`（仅 CRLF normalization warning）。
