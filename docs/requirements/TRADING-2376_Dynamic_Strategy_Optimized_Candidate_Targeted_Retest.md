# TRADING-2376 Dynamic Strategy Optimized Candidate Targeted Retest

最后更新：2026-07-05

## 背景

TRADING-2375 已完成 dynamic strategy candidate optimization and ranking-robustness
divergence review。真实运行结论为
`DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY`，
并把 `dynamic_regime_overlay_v0_4_lower_turnover` 选为下一步 targeted retest 候选。

2375 仍保留 `recommended_decision_after_optimization=OWNER_REVIEW_REQUIRED`，因此 2376
不能直接把候选接入 research-only observation、paper-shadow、scheduler 或 broker path。

## 范围

本任务新增 `aits research strategies dynamic-strategy-optimized-candidate-targeted-retest`，
读取 TRADING-2365、TRADING-2366 和 TRADING-2375 artifacts，重新运行数据质量门禁和
targeted retest。主候选固定为：

```text
dynamic_regime_overlay_v0_4_lower_turnover
```

主执行口径固定为：

```yaml
primary_execution_cadence: valid_until_window
monthly_rebalance:
  allowed_for_reference: true
  allowed_for_primary_decision: false
```

## 允许与禁止

允许：

- 读取既有 market data、strategy configs 和 research artifacts。
- 运行 targeted retest / actual-path backtest。
- 计算 time-slice、regime-slice、cost stress、execution constraint stress 和 ablation metrics。
- 生成 research-only JSON / Markdown 报告。

禁止：

- 启用 scheduler、append event、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 进入 production、调用 broker API、生成 order 或生成 daily report。
- 把 monthly rebalance 作为 primary decision evidence。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source validation|DONE|fail-closed 校验 2365 / 2366 / 2375 ready status、2375 route、primary candidate、primary cadence 和 safety fields。|
|2. Data quality gate|DONE|运行与 `aits validate-data --as-of 2026-07-05` 同源的 cached-data gate，并在 payload / report 中披露。|
|3. Targeted retest matrix|DONE|生成 time-slice、regime-slice、cost stress 和 execution constraint stress metrics。|
|4. Ablation report|DONE|测试 no lower-turnover guardrail、no valid-until window、no cooldown、no risk cap、no constraint filter、no growth tilt / risk overlay。|
|5. Decision update|DONE|输出 `candidate_decision_after_targeted_retest` 和 TRADING-2377 owner review route。|
|6. Registry and docs|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc 和 research docs。|
|7. Validation and closeout|DONE|通过 focused parallel pytest、真实 CLI run、`aits validate-data --as-of 2026-07-05`、docs/report/task-register/contract gates、diff check、isolated commit 和 push。|

## Retest Policy

2376 使用 research-only pilot targeted-retest policy。时间切片使用全样本、三等分样本、
2024-01-01 之后样本、高波动日期集合和回撤恢复日期集合；regime 切片使用 QQQ rolling
volatility、trend、drawdown 和 recovery labels。Ablation 是 diagnostic target-path
transformation，不创建正式策略配置。

这些阈值和切片只用于 owner review 前的研究证据，不是 production policy。若 2376 输出
`ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION`，仍必须进入 TRADING-2377 owner review and
observation decision，不能自动接入 paper-shadow、scheduler 或 broker。

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_optimized_candidate_targeted_retest/targeted_retest_result.json
outputs/research_strategies/dynamic_strategy_optimized_candidate_targeted_retest/time_regime_slice_matrix.json
outputs/research_strategies/dynamic_strategy_optimized_candidate_targeted_retest/ablation_test_report.json
outputs/research_strategies/dynamic_strategy_optimized_candidate_targeted_retest/decision_update.json
docs/research/dynamic_strategy_optimized_candidate_targeted_retest.md
docs/research/dynamic_strategy_targeted_retest_slice_report.md
docs/research/dynamic_strategy_targeted_retest_ablation_report.md
docs/research/dynamic_strategy_2377_route.md
```

## 验收标准

- CLI 返回 `DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY`。
- 输出包含 `primary_candidate=dynamic_regime_overlay_v0_4_lower_turnover`、
  `decision_from_2375=OWNER_REVIEW_REQUIRED` 和 `primary_execution_cadence=valid_until_window`。
- 输出包含 targeted retest、time-slice、regime-slice、cost stress、execution constraint
  stress、ablation 和 decision update。
- 输出明确回答是否仍优于 static baseline、是否穿越 time slices / market regimes、是否
  穿越 realistic / conservative cost、guardrails 是否有实际贡献、是否可升级到
  `ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION`。
- scheduler、event append、outcome binding、paper-shadow、production、broker、daily report
  全部保持 false / none。
- 下一步 route 固定为
  `TRADING-2377_Dynamic_Strategy_Targeted_Retest_Owner_Review_And_Observation_Decision`。

## 进展记录

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 strategy research
  targeted retest，不是 research-only observation activation、paper-shadow approval、
  scheduler enablement、production 或 broker readiness。
- 2026-07-05: 实现完成并进入 closeout validation。真实 CLI run status=`DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY`；primary candidate=`dynamic_regime_overlay_v0_4_lower_turnover`；realistic / conservative / harsh gaps 均为正，但 time_slice_pass_rate=`0.428571`、regime_slice_pass_rate=`0.0`，且相对 2365 ranking top 的 return gap=`-0.019097`。因此 `candidate_decision_after_targeted_retest=CONTINUE_OPTIMIZATION`，不是 `ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION`；scheduler、event append、outcome binding、paper-shadow、production、broker 和 daily report 全部保持 disabled / false / none。
