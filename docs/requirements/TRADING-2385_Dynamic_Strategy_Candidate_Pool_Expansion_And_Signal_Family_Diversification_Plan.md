# TRADING-2385 Dynamic Strategy Candidate Pool Expansion And Signal Family Diversification Plan

最后更新：2026-07-06

## 背景

TRADING-2384 已完成 guarded variant owner review and observation decision，真实状态为
`DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。

2384 明确不批准 research-only observation，并记录：

- lower-turnover line best variant=`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`，decision=`CONTINUE_OPTIMIZATION`，observation=false
- ranking-top guarded line best variant=`equal_risk_growth_tilt_guarded_turnover_v1`，decision=`CONTINUE_OPTIMIZATION`，observation=false
- owner decision=`DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED`
- next route=`TRADING-2385_Dynamic_Strategy_Candidate_Pool_Expansion_And_Signal_Family_Diversification_Plan`

2385 的目标是把 2384 的 owner decision 转成 2386 expanded candidate pool retest 前的
plan-only artifact，避免继续围绕两条局部优化线做边际调参。

## 范围

必须执行：

- 读取 TRADING-2384 owner review decision artifacts。
- 读取 TRADING-2383 guarded variant retest artifacts。
- 读取 TRADING-2379 lower-turnover optimized variant retest artifacts。
- 读取 TRADING-2365 candidate ranking artifacts。
- 读取 TRADING-2366 cost / turnover / cooldown sensitivity artifacts。
- 汇总现有两条候选线为何不足以继续局部优化。
- 定义 reference candidates、candidate pool expansion families、signal family diversification plan。
- 定义 candidate budget / anti-overfit guardrails。
- 定义 TRADING-2386 expanded candidate pool retest plan、stress tests 和 acceptance criteria。

明确禁止：

- 启用 scheduler。
- append historical event log。
- bind / mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 启用 production、调用 broker API 或生成 order。
- 生成 daily report。
- 运行新 backtest。
- 生成新 signal。

## Data Quality Gate

本任务只读取 prior validated TRADING-2365 / 2366 / 2379 / 2383 / 2384 artifacts，
不读取 fresh cached market data、不重新运行 backtest、不生成 technical features、
scoring、daily report 或交易建议，因此不重跑 `aits validate-data`。

必须在输出中披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA`

## 默认计划

- primary execution cadence：`valid_until_window`
- next route：`TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening`
- candidate families：
  - `regime_transition_family`
  - `trend_confirmation_family`
  - `volatility_aware_family`
  - `signal_age_valid_until_family`
  - `turnover_budget_family`
  - `risk_cap_interaction_family`
- reference candidates：
  - `static_baseline`
  - `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
  - `dynamic_regime_overlay_v0_4_lower_turnover`
  - `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
  - `equal_risk_growth_tilt_guarded_turnover_v1`

## 输出

JSON：

- `outputs/research_strategies/dynamic_strategy_candidate_pool_expansion_plan/candidate_pool_expansion_plan.json`
- `outputs/research_strategies/dynamic_strategy_candidate_pool_expansion_plan/signal_family_diversification_plan.json`
- `outputs/research_strategies/dynamic_strategy_candidate_pool_expansion_plan/candidate_budget_guardrails.json`
- `outputs/research_strategies/dynamic_strategy_candidate_pool_expansion_plan/retest_plan_2386.json`

Markdown：

- `docs/research/dynamic_strategy_candidate_pool_expansion_plan.md`
- `docs/research/dynamic_strategy_signal_family_diversification_plan.md`
- `docs/research/dynamic_strategy_candidate_budget_and_anti_overfit_guardrails.md`
- `docs/research/dynamic_strategy_2386_route.md`

## 验收标准

- CLI `aits research strategies dynamic-strategy-candidate-pool-expansion-plan`
  返回 `DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN_READY`。
- 输出包含 candidate pool expansion plan、signal family diversification plan、
  reference candidates、candidate budget guardrails、anti-overfit guardrails、
  retest_plan_2386 和 recommended next task。
- `candidate_pool_expansion_recommended=true`。
- `signal_family_diversification_recommended=true`。
- `candidate_budget_ready=true`。
- `anti_overfit_guardrails_ready=true`。
- `retest_plan_2386_ready=true`。
- `primary_execution_cadence=valid_until_window`。
- monthly rebalance 不得作为 primary decision。
- scheduler、event append、outcome binding、paper-shadow、production、broker、
  order 和 daily report 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive
  同步更新。
- focused tests、Ruff、compileall、docs freshness、documentation contract、
  task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件登记并进入 `IN_PROGRESS`。本任务是 candidate
  pool expansion and signal family diversification plan；不是 expanded candidate
  pool retest、不是新 backtest、不是新 signal、不是 research-only observation、
  paper-shadow、scheduler、production 或 broker readiness。
- 2026-07-06：实现完成并进入归档。新增
  `aits research strategies dynamic-strategy-candidate-pool-expansion-plan`、
  builder、focused tests、registry、artifact catalog、system flow 和 research docs；
  真实 CLI run 返回
  `DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN_READY`，
  reference candidates=5、signal families=6、2386 默认 new candidates=12，
  candidate budget / anti-overfit guardrails / retest plan 均 ready。仍未重跑
  `aits validate-data`，因为本任务只读取 prior validated artifacts，不读取 fresh
  cached market data、不重新 backtest、不生成 signal/scoring/daily report。
- 2026-07-06：最终验证通过 full Ruff、`compileall -q src tests`、focused
  parallel pytest 3 passed、真实 CLI run、docs freshness 570 docs PASS、
  documentation contract 1282 reports PASS、task-register consistency run
  active=319 / completed=444 / failed=0、task-register consistency validate
  checks=5 / failed=0 / warnings=0、contract-validation 197 passed（runtime
  artifact=`outputs/validation_runtime/contract-validation_20260705T163743Z/test_runtime_summary.json`）
  和 `git diff --check`（仅 CRLF normalization warning）。
