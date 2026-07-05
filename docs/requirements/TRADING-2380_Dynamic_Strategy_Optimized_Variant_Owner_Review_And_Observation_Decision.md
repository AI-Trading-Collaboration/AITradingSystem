# TRADING-2380 Dynamic Strategy Optimized Variant Owner Review And Observation Decision

最后更新：2026-07-05

## 背景

TRADING-2379 已完成 slice-robustness optimized variant retest，真实状态为
`DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY`。

2379 的 best variant 为
`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`，但 best decision 仍为
`CONTINUE_OPTIMIZATION`，且 next route 已固定为
`TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_Observation_Decision`。

## 范围

本任务新增
`aits research strategies dynamic-strategy-optimized-variant-owner-review-decision`，
只读取 TRADING-2376 / 2378 / 2379 prior artifacts，记录 owner review decision：
不批准 research-only observation，不批准 paper-shadow，不批准任何执行链路；允许继续策略研究优化，
但下一步必须先进入 optimization plateau / next candidate decision review。

默认 owner decision：

```text
DO_NOT_APPROVE_OBSERVATION_CONTINUE_OPTIMIZATION_REVIEW_REQUIRED
```

## 允许与禁止

允许：

- 读取既有 2376 targeted retest artifacts。
- 读取既有 2378 optimization plan artifacts。
- 读取既有 2379 optimized variant retest artifacts。
- 记录 owner review decision。
- 生成 observation rejection rationale。
- 生成 continue-optimization / plateau review route。

禁止：

- 启用 scheduler、创建 scheduled task 或生成 daily report。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 进入 production、调用 broker API 或生成 order。
- 把本 owner review record 解读为 observation / paper-shadow / production approval。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source registration and validation|DONE|登记任务；fail-closed 校验 2376 / 2378 / 2379 source status、2379 route、best variant、best decision、primary cadence 和 safety fields。|
|2. Owner decision record|DONE|生成 owner decision record，明确 `research_only_observation_approved=false`、`continue_optimization_allowed=true`、`optimization_plateau_review_required=true`。|
|3. Rejection rationale and route|DONE|生成 observation rejection reasons、continue-optimization rationale、plateau review requirement 和 TRADING-2381 route。|
|4. Registry, docs and validation|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc、research docs、focused tests，并完成 full validation / commit / push。|

## Data Quality Gate

2380 只读取 prior validated artifacts，不读取 fresh cached market data、不重新 backtest、不生成
technical features、scoring、daily report 或交易建议，因此 `aits validate-data` 不适用。
输出必须披露：

```text
data_quality_gate_executed=false
data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REVIEW_ONLY_NO_FRESH_MARKET_DATA
```

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_optimized_variant_owner_review_decision/owner_review_decision.json
outputs/research_strategies/dynamic_strategy_optimized_variant_owner_review_decision/observation_rejection_rationale.json
docs/research/dynamic_strategy_optimized_variant_owner_review_decision.md
docs/research/dynamic_strategy_observation_rejection_rationale.md
docs/research/dynamic_strategy_2381_route.md
```

## 验收标准

- CLI 返回 `DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。
- `best_variant_from_2379=dynamic_regime_overlay_v0_4_cooldown_balanced_v1`。
- `best_variant_decision_from_2379=CONTINUE_OPTIMIZATION`。
- `owner_decision=DO_NOT_APPROVE_OBSERVATION_CONTINUE_OPTIMIZATION_REVIEW_REQUIRED`。
- `research_only_observation_approved=false`。
- `continue_optimization_allowed=true`。
- `optimization_plateau_review_required=true`。
- 输出 observation rejection reasons 和 TRADING-2381 route。
- scheduler、event append、outcome binding、paper-shadow、paper trade、shadow position、
  production、broker、daily report 全部保持 false / none。

## 进展记录

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 owner review
  / observation decision record，不是 observation approval、paper-shadow approval、scheduler
  enablement、production 或 broker readiness。
- 2026-07-05: 实现完成并归档 `DONE`。新增 optimized variant owner review
  decision CLI、builder、observation rejection rationale、TRADING-2381 route、research docs、
  registry、catalog、system flow 和 focused tests。真实 run status 为
  `DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`，
  owner decision 为
  `DO_NOT_APPROVE_OBSERVATION_CONTINUE_OPTIMIZATION_REVIEW_REQUIRED`，
  research-only observation approved=false，continue optimization allowed=true，
  optimization plateau review required=true，next route 为
  `TRADING-2381_Dynamic_Strategy_Optimization_Plateau_And_Next_Candidate_Decision`。
- 2026-07-05: closeout 验证通过 full Ruff、`compileall -q src tests`、
  focused parallel pytest 3 passed、真实 CLI run、docs freshness 565 docs PASS、
  documentation contract 1277 reports PASS、task-register consistency run/validate
  PASS、contract-validation 197 passed（runtime artifact：
  `outputs/validation_runtime/contract-validation_20260705T150254Z/test_runtime_summary.json`）
  和 `git diff --check`（仅 CRLF normalization warning）。本任务未重跑
  `aits validate-data`，因为只读取 prior validated artifacts，不读取 fresh cached market data。
