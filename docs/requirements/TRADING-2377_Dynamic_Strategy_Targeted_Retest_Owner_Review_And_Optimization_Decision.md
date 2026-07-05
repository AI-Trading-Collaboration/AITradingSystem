# TRADING-2377 Dynamic Strategy Targeted Retest Owner Review And Optimization Decision

最后更新：2026-07-05

## 背景

TRADING-2376 已完成 optimized candidate targeted retest。真实运行状态为
`DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY`，主候选为
`dynamic_regime_overlay_v0_4_lower_turnover`，且 `candidate_decision_after_targeted_retest`
为 `CONTINUE_OPTIMIZATION`。

2376 结果显示该候选在 realistic / conservative / harsh cost stress 下仍为正 gap，
但 time-slice、regime-slice 稳定性不足，且相对 TRADING-2365 ranking top 的 return gap
未修复。因此 2377 不能把候选升级到 research-only observation，也不能进入 paper-shadow、
event append、outcome binding、scheduler、production 或 broker path。

## 范围

本任务新增 `aits research strategies dynamic-strategy-targeted-retest-owner-review-decision`，
读取 TRADING-2365、TRADING-2366、TRADING-2375 和 TRADING-2376 artifacts，记录 owner
review decision 和 continue-optimization decision package。

默认 owner decision 固定为：

```text
KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION
```

含义：候选仍有研究价值，但 targeted retest 证据不足以进入 research-only observation；
下一步应围绕 time/regime slice robustness 和 return gap repair 做优化。

## 允许与禁止

允许：

- 读取既有 TRADING-2365 / 2366 / 2375 / 2376 research artifacts。
- 校验 2376 decision=`CONTINUE_OPTIMIZATION`。
- 生成 owner review decision record、continue-optimization gate 和 research Markdown。
- 生成 TRADING-2378 optimization route。

禁止：

- 启用 scheduler、创建 scheduled task 或生成 daily report。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 进入 production、调用 broker API 或生成 order。
- 重新运行 backtest 或读取 fresh cached market data。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source validation|DONE|fail-closed 校验 2376 ready status、decision=`CONTINUE_OPTIMIZATION`、primary candidate、primary cadence、2375 source 和 safety fields。|
|2. Owner decision package|DONE|输出 owner decision、research-only observation non-approval、continue-optimization approval 和 explicit non-approval list。|
|3. Optimization route|DONE|输出 time/regime slice robustness、return-gap repair、upside capture 和 valid-until window tuning focus，下一步 route=`TRADING-2378_Dynamic_Strategy_Slice_Robustness_And_Return_Gap_Optimization_Plan`。|
|4. Registry and docs|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc 和 research docs。|
|5. Validation and closeout|DONE|通过 focused parallel pytest、真实 CLI run、docs/report/task-register/contract gates、diff check、isolated commit 和 push。|

## Data Quality Gate 说明

2377 只读取 prior validated research artifacts，不读取 fresh cached market data、不重新 backtest、
不生成 technical features、scoring、daily report 或交易建议。因此本任务不要求重新运行
`aits validate-data --as-of 2026-07-05`。输出必须披露：

```text
data_quality_gate_executed=false
data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REVIEW_ONLY_NO_FRESH_MARKET_DATA
```

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_targeted_retest_owner_review_decision/owner_review_decision.json
outputs/research_strategies/dynamic_strategy_targeted_retest_owner_review_decision/continue_optimization_gate.json
docs/research/dynamic_strategy_targeted_retest_owner_review_decision.md
docs/research/dynamic_strategy_continue_optimization_decision.md
docs/research/dynamic_strategy_2378_route.md
```

## 验收标准

- CLI 返回
  `DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_OPTIMIZATION_DECISION_READY`。
- 输出包含 `primary_candidate=dynamic_regime_overlay_v0_4_lower_turnover`、
  `decision_from_2376=CONTINUE_OPTIMIZATION` 和
  `primary_execution_cadence=valid_until_window`。
- 输出包含 `owner_review_decision_recorded=true`、
  `owner_decision=KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION`、
  `research_only_observation_approved=false` 和 `continue_optimization_approved=true`。
- 输出明确回答不批准 research-only observation、paper-shadow、paper trade、shadow position、
  event append、outcome binding、scheduler、production、broker 和 daily report。
- 输出包含 optimization focus：time slice robustness、regime slice robustness、
  return gap repair、upside capture without turnover increase、valid-until window parameter tuning。
- 下一步 route 固定为
  `TRADING-2378_Dynamic_Strategy_Slice_Robustness_And_Return_Gap_Optimization_Plan`。

## 进展记录

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 owner review /
  optimization decision record，不是 research-only observation activation、paper-shadow
  approval、scheduler enablement、production 或 broker readiness。
- 2026-07-05: 实现完成并进入 closeout。真实 CLI run status=
  `DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_OPTIMIZATION_DECISION_READY`；
  owner_decision=`KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION`；
  research_only_observation_approved=false；continue_optimization_approved=true；
  next route=`TRADING-2378_Dynamic_Strategy_Slice_Robustness_And_Return_Gap_Optimization_Plan`。
  验证通过 full Ruff、`compileall -q src tests`、focused parallel pytest 3 passed、
  docs freshness、documentation contract、task-register consistency、contract-validation 197 passed
  和 `git diff --check`。本任务未重跑 `aits validate-data --as-of 2026-07-05`，
  因为只读取 prior validated TRADING-2365 / 2366 / 2375 / 2376 artifacts，不读取
  fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report
  或交易建议。
