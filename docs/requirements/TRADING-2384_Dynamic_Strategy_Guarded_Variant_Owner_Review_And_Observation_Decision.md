# TRADING-2384 Dynamic Strategy Guarded Variant Owner Review And Observation Decision

最后更新：2026-07-06

## 背景

TRADING-2383 已完成 ranking-top guarded variant retest，真实状态为
`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY`。

2383 的最佳 guarded variant 为 `equal_risk_growth_tilt_guarded_turnover_v1`，
decision=`CONTINUE_OPTIMIZATION`，且
`candidate_ready_for_research_only_observation=false`。结合 TRADING-2379 / 2380
lower-turnover 优化线同样未批准 observation，当前动态策略候选仍有研究价值，
但没有候选达到 research-only observation 门槛。

## 范围

必须执行：

- 读取 TRADING-2383 guarded variant retest artifacts。
- 读取 TRADING-2382 ranking top guarded retest plan artifacts。
- 读取 TRADING-2381 optimization plateau / next direction artifacts。
- 读取 TRADING-2379 lower-turnover optimized variant retest artifacts。
- 读取 TRADING-2380 lower-turnover owner review decision artifacts。
- 汇总 lower-turnover line 与 ranking-top guarded line。
- 记录 owner decision：
  `DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED`。
- 生成 observation rejection rationale、two-line candidate review、next research
  direction decision 和 TRADING-2385 route。

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

本任务只读取 prior validated TRADING-2379 / 2380 / 2381 / 2382 / 2383 artifacts，
不读取 fresh cached market data、不重新运行 backtest、不生成 technical features、
scoring、daily report 或交易建议，因此不重跑 `aits validate-data`。

必须在输出中披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REVIEW_ONLY_NO_FRESH_MARKET_DATA`

## 默认决策

- owner decision：
  `DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED`
- next direction：
  `OPTION_C_EXPAND_CANDIDATE_POOL_AND_SIGNAL_FAMILIES`
- next route：
  `TRADING-2385_Dynamic_Strategy_Candidate_Pool_Expansion_And_Signal_Family_Diversification_Plan`

## 输出

JSON：

- `outputs/research_strategies/dynamic_strategy_guarded_variant_owner_review_decision/owner_review_decision.json`
- `outputs/research_strategies/dynamic_strategy_guarded_variant_owner_review_decision/two_line_candidate_review.json`
- `outputs/research_strategies/dynamic_strategy_guarded_variant_owner_review_decision/next_research_direction_decision.json`

Markdown：

- `docs/research/dynamic_strategy_guarded_variant_owner_review_decision.md`
- `docs/research/dynamic_strategy_two_line_candidate_review.md`
- `docs/research/dynamic_strategy_observation_rejection_after_guarded_retest.md`
- `docs/research/dynamic_strategy_2385_route.md`

## 验收标准

- CLI `aits research strategies dynamic-strategy-guarded-variant-owner-review-decision`
  返回 `DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。
- 输出包含 `lower_turnover_line`、`ranking_top_guarded_line`、`owner_decision`、
  `two_line_candidate_review`、`next_research_direction_decision` 和
  `recommended_next_research_task`。
- `research_only_observation_approved=false`。
- `candidate_pool_expansion_recommended=true`。
- `signal_family_diversification_recommended=true`。
- `continue_local_optimization_allowed=false`。
- `primary_execution_cadence=valid_until_window`。
- scheduler、event append、outcome binding、paper-shadow、production、broker、
  order 和 daily report 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive
  同步更新。
- focused tests、Ruff、compileall、docs freshness、documentation contract、
  task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件登记并进入 `IN_PROGRESS`。本任务是 owner review /
  observation decision record；不得把结论解释为 research-only observation、
  paper-shadow、scheduler、production 或 broker readiness。
- 2026-07-06：实现完成并进入 `DONE`。新增
  `aits research strategies dynamic-strategy-guarded-variant-owner-review-decision`、
  guarded variant owner review decision builder、two-line candidate review、
  observation rejection after guarded retest、next direction decision、TRADING-2385
  route、research docs、registry、catalog、system flow 和 focused tests；真实 run
  status=`DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。
  本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2379 /
  2380 / 2381 / 2382 / 2383 artifacts，不读取 fresh cached market data、不重新
  backtest、不生成 technical features、scoring、daily report 或交易建议。
