# TRADING-2386 Dynamic Strategy Expanded Candidate Pool Retest And Screening

最后更新：2026-07-06

## 背景

TRADING-2385 已完成 candidate pool expansion and signal family diversification plan，
真实状态为
`DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN_READY`。

2385 明确：

- candidate pool expansion recommended=`true`
- signal family diversification recommended=`true`
- reference candidates=5
- signal families=6
- default new candidates for 2386=12
- next route=`TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening`

2386 的目标是对 2385 预注册的 expanded candidate pool 执行实际 retest /
screening，判断扩展候选池中是否出现优于既有 reference line 的动态策略候选。

## 范围

必须执行：

- 读取 TRADING-2385 candidate pool expansion plan artifacts。
- 读取 TRADING-2384 owner decision artifacts。
- 读取 TRADING-2383 guarded variant retest artifacts。
- 读取 TRADING-2379 lower-turnover optimized variant retest artifacts。
- 读取 TRADING-2365 candidate ranking artifacts。
- 读取 TRADING-2366 cost / turnover / cooldown sensitivity artifacts。
- 运行 `aits validate-data --as-of 2026-07-05`。
- 构建 5 个 reference candidates 和最多 12 个 new candidates。
- 按 `valid_until_window` 主口径运行 expanded candidate pool retest。
- 输出 expanded candidate ranking、signal family screening、cost stress、
  turnover / constraint stress、time/regime slice matrix 和 decision update。

明确禁止：

- 启用 scheduler。
- append historical event log。
- bind / mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 启用 production、调用 broker API 或生成 order。
- 生成 daily report。

## Data Quality Gate

本任务会读取 cached market data 并运行 actual retest / backtest，必须先执行：

```bash
aits validate-data --as-of 2026-07-05
```

CLI 输出必须披露 data-quality status、quality report path 和 gate 结果。

## 默认约束

- primary execution cadence：`valid_until_window`
- comparison cadences：`valid_until_window`、`cooldown_limited_event_driven`、
  `signal_event_driven`
- monthly rebalance：只允许 legacy reference，不得作为 primary decision
- next route：
  `TRADING-2387_Dynamic_Strategy_Expanded_Candidate_Owner_Review_And_Next_Research_Decision`

## 输出

JSON：

- `outputs/research_strategies/dynamic_strategy_expanded_candidate_pool_retest/expanded_candidate_retest_result.json`
- `outputs/research_strategies/dynamic_strategy_expanded_candidate_pool_retest/expanded_candidate_ranking.json`
- `outputs/research_strategies/dynamic_strategy_expanded_candidate_pool_retest/signal_family_screening.json`
- `outputs/research_strategies/dynamic_strategy_expanded_candidate_pool_retest/time_regime_slice_matrix.json`
- `outputs/research_strategies/dynamic_strategy_expanded_candidate_pool_retest/decision_update.json`

Markdown：

- `docs/research/dynamic_strategy_expanded_candidate_pool_retest.md`
- `docs/research/dynamic_strategy_expanded_candidate_ranking.md`
- `docs/research/dynamic_strategy_signal_family_screening.md`
- `docs/research/dynamic_strategy_2387_route.md`

## 验收标准

- CLI `aits research strategies dynamic-strategy-expanded-candidate-pool-retest`
  返回 `DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY`。
- `aits validate-data --as-of 2026-07-05` 已执行并通过。
- 输出包含 reference candidates、signal families tested、expanded candidate
  ranking、signal family screening、time/regime slice matrix、cost stress result、
  decision update、best candidate、best signal family 和 next route。
- `primary_execution_cadence=valid_until_window`。
- monthly rebalance 不得作为 primary decision。
- scheduler、event append、outcome binding、paper-shadow、production、broker、
  order 和 daily report 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive
  同步更新。
- focused tests、Ruff、compileall、docs freshness、documentation contract、
  task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件登记并进入 `IN_PROGRESS`。本任务是 expanded
  candidate pool actual retest / screening，必须运行 cached-data quality gate；
  不是 observation approval、paper-shadow、scheduler、production 或 broker
  readiness。
- 2026-07-06：实现完成并归档 `DONE`。新增 expanded candidate pool retest CLI /
  builder / focused tests，生成 required JSON 与 Markdown artifacts；真实 run
  status=`DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY`，
  data quality=`PASS_WITH_WARNINGS` / errors=0 / warnings=2，reference
  candidates=5，new candidates tested=12，signal families tested=6，best
  candidate=`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`，best
  decision=`CONTINUE_OPTIMIZATION`，未发现 observation-ready 新候选，next
  route=`TRADING-2387_Dynamic_Strategy_Expanded_Candidate_Owner_Review_And_Next_Research_Decision`。
  scheduler、event append、outcome binding、paper-shadow、paper trade、shadow
  position、production、broker/order 和 daily report 全部保持 disabled / false /
  none。
