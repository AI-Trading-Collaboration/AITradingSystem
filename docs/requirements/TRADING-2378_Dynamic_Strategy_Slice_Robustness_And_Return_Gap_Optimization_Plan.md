# TRADING-2378 Dynamic Strategy Slice Robustness And Return Gap Optimization Plan

最后更新：2026-07-05

## 背景

TRADING-2377 已完成 targeted retest owner review and optimization decision。真实运行状态为
`DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_OPTIMIZATION_DECISION_READY`，
owner decision 为 `KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION`，并把下一步 route
固定为 `TRADING-2378_Dynamic_Strategy_Slice_Robustness_And_Return_Gap_Optimization_Plan`。

TRADING-2376 targeted retest 显示 `dynamic_regime_overlay_v0_4_lower_turnover`
在 realistic / conservative / harsh cost stress 下仍保留正 gap，但 time-slice 和
regime-slice 稳定性不足，且相对
`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` 的 return gap 未修复。

## 范围

本任务新增
`aits research strategies dynamic-strategy-slice-robustness-return-gap-optimization-plan`，
只读取 TRADING-2365 / 2366 / 2375 / 2376 / 2377 prior artifacts，生成：

- time-slice gap diagnosis
- regime-slice gap diagnosis
- return gap repair plan
- optimized variant plan
- TRADING-2379 variant evaluation plan

2378 是 optimization plan，不是回测执行任务。

## 允许与禁止

允许：

- 读取既有 research artifacts。
- 诊断 time/regime slice failure source。
- 拆解 return gap vs ranking top。
- 生成待 2379 retest 的 variant plan 和 evaluation criteria。
- 生成 research-only JSON / Markdown 报告。

禁止：

- 读取 fresh cached market data 或重新运行 backtest。
- 启用 scheduler、创建 scheduled task 或生成 daily report。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 进入 production、调用 broker API 或生成 order。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source validation|DONE|fail-closed 校验 2377 / 2376 / 2375 / 2366 / 2365 source status、route、primary candidate、ranking top、primary cadence、owner decision 和 safety fields。|
|2. Slice diagnosis|DONE|输出 time-slice 和 regime-slice failure diagnosis，包含 affected slice/regime、gap、likely cause、proposed fix、impact 和 retest requirement。|
|3. Return-gap repair plan|DONE|拆解 upside capture、risk-on weight、reentry delay、defensive floor、valid-until expiry、cooldown、turnover cap 和 growth tilt intensity gaps。|
|4. Variant plan|DONE|生成 base reference 与至少 5 个 optimized variants，明确 expected benefit/cost/risk/turnover impact。|
|5. Registry and docs|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc、docs/tasks/completed 和 research docs。|
|6. Validation and closeout|DONE|通过 focused parallel pytest、真实 CLI run、docs/report/task-register/contract gates、diff check、isolated commit 和 push。|

## Data Quality Gate 说明

2378 只读取 prior validated research artifacts，不读取 fresh cached market data、不重新 backtest、
不生成 technical features、scoring、daily report 或交易建议。因此本任务不要求重新运行
`aits validate-data --as-of 2026-07-05`。输出必须披露：

```text
data_quality_gate_executed=false
data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_OPTIMIZATION_PLAN_ONLY_NO_FRESH_MARKET_DATA
```

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_slice_robustness_return_gap_optimization_plan/optimization_plan_result.json
outputs/research_strategies/dynamic_strategy_slice_robustness_return_gap_optimization_plan/time_regime_slice_gap_diagnosis.json
outputs/research_strategies/dynamic_strategy_slice_robustness_return_gap_optimization_plan/return_gap_repair_variant_plan.json
outputs/research_strategies/dynamic_strategy_slice_robustness_return_gap_optimization_plan/variant_evaluation_plan.json
docs/research/dynamic_strategy_slice_robustness_return_gap_optimization_plan.md
docs/research/dynamic_strategy_time_regime_slice_gap_diagnosis.md
docs/research/dynamic_strategy_return_gap_repair_variant_plan.md
docs/research/dynamic_strategy_2379_route.md
```

## 验收标准

- CLI 返回
  `DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY`。
- 输出包含 `primary_candidate=dynamic_regime_overlay_v0_4_lower_turnover`、
  `ranking_top_reference=equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`、
  `decision_from_2377=KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION` 和
  `primary_execution_cadence=valid_until_window`。
- 输出包含 time-slice gap diagnosis、regime-slice gap diagnosis、return-gap repair plan、
  variant plan 和 2379 evaluation plan。
- planned variants 至少包含 reentry repair、upside capture guarded、valid-until decay tuned、
  cooldown balanced 和 return-gap repair fusion。
- forbidden optimization paths 明确包含移除 lower-turnover guardrail、使用 monthly rebalance
  作为 primary、stale signal carry-forward、无 risk cap 增长 tilt、完全移除 cooldown、
  不做 cost stress 增加 turnover、只优化 total return、忽略 drawdown/regime failure。
- scheduler、event append、outcome binding、paper-shadow、production、broker、daily report
  全部保持 false / none。
- 下一步 route 固定为
  `TRADING-2379_Dynamic_Strategy_Slice_Robustness_Optimized_Variant_Retest`。

## 进展记录

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 research-only
  optimization plan，不是 optimized variant retest、research-only observation activation、
  paper-shadow approval、scheduler enablement、production 或 broker readiness。
- 2026-07-05: 实现完成并进入 `DONE`。新增 CLI、optimization plan builder、
  time/regime slice gap diagnosis、return-gap repair variant plan、variant evaluation plan、
  research docs、registry、catalog、system flow、task register/archive 和 focused tests；
  真实 run status 为
  `DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY`，
  next route 固定为
  `TRADING-2379_Dynamic_Strategy_Slice_Robustness_Optimized_Variant_Retest`。
