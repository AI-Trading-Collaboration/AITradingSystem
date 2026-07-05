# TRADING-2383 Dynamic Strategy Ranking Top Guarded Variant Retest

最后更新：2026-07-06

## 背景

TRADING-2382 已完成 ranking top guarded-turnover retest plan，真实状态为
`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY`，并把
TRADING-2383 定义为实际 guarded ranking-top variants retest。

本任务从 TRADING-2365 的收益 top
`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` 出发，引入 TRADING-2382
规划的 turnover、cooldown、risk-cap、valid-until 与 lower-turnover fusion
guardrails，实际运行回测并判断是否有候选可以进入 research-only observation
owner review。

## 范围

必须执行：

- 读取 TRADING-2382 retest plan artifacts。
- 读取 TRADING-2365 candidate ranking artifacts。
- 读取 TRADING-2366 cost / turnover sensitivity artifacts。
- 读取 TRADING-2379 lower-turnover variant retest artifacts。
- 执行 `aits validate-data --as-of 2026-07-05` 或同源 data quality gate。
- 以 `valid_until_window` 为主口径运行 guarded ranking-top variant retest。
- 输出 guarded variant ranking、time/regime slice matrix、cost stress、turnover
  constraint stress 和 decision update。
- 输出 TRADING-2384 owner review route。

明确禁止：

- 启用 scheduler。
- append historical event log。
- bind / mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 启用 production、调用 broker API 或生成 order。
- 生成 daily report。

## 待测候选

- `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- `equal_risk_growth_tilt_guarded_turnover_v1`
- `equal_risk_growth_tilt_guarded_cooldown_v1`
- `equal_risk_growth_tilt_guarded_risk_cap_v1`
- `equal_risk_growth_tilt_guarded_valid_until_decay_v1`
- `equal_risk_growth_tilt_lower_turnover_fusion_v1`
- `equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1`

对照候选：

- `static_baseline`
- `dynamic_regime_overlay_v0_4_lower_turnover`
- `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`

## 输出

JSON：

- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/guarded_variant_retest_result.json`
- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/guarded_variant_ranking.json`
- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/time_regime_slice_matrix.json`
- `outputs/research_strategies/dynamic_strategy_ranking_top_guarded_variant_retest/decision_update.json`

Markdown：

- `docs/research/dynamic_strategy_ranking_top_guarded_variant_retest.md`
- `docs/research/dynamic_strategy_guarded_variant_ranking.md`
- `docs/research/dynamic_strategy_guarded_variant_slice_matrix.md`
- `docs/research/dynamic_strategy_2384_route.md`

## 验收标准

- CLI `aits research strategies dynamic-strategy-ranking-top-guarded-variant-retest`
  返回 `DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY`。
- `data_quality_gate_executed=true`，且 data quality gate 已在真实 run 中通过或
  PASS_WITH_WARNINGS 且 errors=0。
- 输出 `variants_tested`、`guarded_variant_ranking`、`time_slice_matrix`、
  `regime_slice_matrix`、`cost_stress_result`、`turnover_constraint_result`、
  `best_guarded_variant`、`best_guarded_variant_decision` 和
  `recommended_next_research_task`。
- `primary_execution_cadence=valid_until_window`。
- monthly rebalance 只允许作为 legacy reference，不得作为 primary decision。
- scheduler、event append、outcome binding、paper-shadow、production、broker、
  order 和 daily report 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive
  同步更新。
- focused tests、Ruff、compileall、docs freshness、documentation contract、
  task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-06：根据 owner 附件登记并进入 `IN_PROGRESS`。本任务会实际运行
  guarded variant retest / backtest，因此必须执行 cached-data quality gate；不得把
  retest 结果解释为 paper-shadow、production 或 broker readiness。
- 2026-07-06：实现完成并归档 `DONE`。新增 CLI、builder、focused tests、
  report registry、artifact catalog、system flow、research docs 和 completed archive。
  真实 run status=`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY`；
  data quality=`PASS_WITH_WARNINGS` / errors=0 / warnings=2；
  best_guarded_variant=`equal_risk_growth_tilt_guarded_turnover_v1`；
  best_guarded_variant_decision=`CONTINUE_OPTIMIZATION`；
  candidate_ready_for_research_only_observation=false；next route=
  `TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_Observation_Decision`。
  scheduler、event append、outcome binding、paper-shadow、paper trade、shadow position、
  production、broker/order 和 daily report 全部保持 disabled / false / none。

## 验证记录

- `python -m ai_trading_system.cli validate-data --as-of 2026-07-05`：PASS_WITH_WARNINGS；
  errors=0；warnings=2。
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_ranking_top_guarded_variant_retest.py`：3 passed。
- `python -m ai_trading_system.cli research strategies dynamic-strategy-ranking-top-guarded-variant-retest --as-of 2026-07-05`：READY。
- `python -m ruff check .`：PASS。
- `python -m compileall -q src tests`：PASS。
- `python -m ai_trading_system.cli docs validate-freshness`：568 docs PASS。
- `python -m ai_trading_system.cli docs report-contract --latest`：1280 reports PASS。
- `python -m ai_trading_system.cli reports task-register-consistency run`：
  active=319 / completed=442 / failed=0。
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：
  checks=5 / failed=0 / warnings=0。
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：
  197 passed；runtime artifact：
  `outputs/validation_runtime/contract-validation_20260705T160431Z/test_runtime_summary.json`。
- `git diff --check`：PASS；仅 CRLF normalization warning。
