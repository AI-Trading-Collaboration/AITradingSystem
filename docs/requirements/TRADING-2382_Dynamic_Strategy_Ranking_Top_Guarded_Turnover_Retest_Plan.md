# TRADING-2382 Dynamic Strategy Ranking Top Guarded-Turnover Retest Plan

最后更新：2026-07-06

## 背景

TRADING-2381 已完成 optimization plateau and next candidate decision，真实状态为
`DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY`。

2381 判断 lower-turnover 优化线仍有稳健性价值，但多轮优化后仍未达到 observation 门槛，
因此下一阶段默认方向切到：

```text
OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT
```

本任务回到 TRADING-2365 的收益 top：

```text
equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1
```

但 2382 只制定 guarded-turnover retest plan，不运行实际 backtest。

## 范围

本任务新增
`aits research strategies dynamic-strategy-ranking-top-guarded-turnover-retest-plan`，
只读取 TRADING-2365 / 2366 / 2379 / 2380 / 2381 prior artifacts，不读取 fresh cached
market data、不重新 backtest、不生成新 signal 或 scoring。任务目标是诊断 ranking top 的收益来源
与脆弱性，迁移 lower-turnover guardrails，生成 guarded ranking-top variants，并定义 2383 retest
evaluation plan。

推荐 next route：

```text
TRADING-2383_Dynamic_Strategy_Ranking_Top_Guarded_Variant_Retest
```

## 允许与禁止

允许：

- 读取既有 TRADING-2365 candidate ranking artifacts。
- 读取既有 TRADING-2366 sensitivity artifacts。
- 读取既有 TRADING-2379 optimized variant retest artifacts。
- 读取既有 TRADING-2380 owner review / observation rejection artifacts。
- 读取既有 TRADING-2381 plateau decision / next direction artifacts。
- 生成 ranking top fragility diagnosis。
- 生成 lower-turnover guardrail transfer plan。
- 生成 guarded ranking-top variant plan。
- 生成 2383 retest evaluation plan 和 research docs。

禁止：

- 运行新 backtest 或生成新 signal。
- 读取 fresh cached market data 或生成 scoring。
- 启用 scheduler、创建 scheduled task 或生成 daily report。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 进入 production、调用 broker API 或生成 order。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source registration and validation|DONE|登记任务；fail-closed 校验 2365 / 2366 / 2379 / 2380 / 2381 source status、2381 next direction / route、ranking top、guardrail references、primary cadence 和 safety fields。|
|2. Fragility and guardrail diagnosis|DONE|生成 ranking top return source / fragility diagnosis 与 lower-turnover transferable guardrail plan。|
|3. Guarded variant and 2383 plan|DONE|生成 guarded ranking-top variants、forbidden optimization paths、2383 execution cadence / cost stress / turnover stress / slice test / acceptance criteria。|
|4. Registry, docs and validation|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc、research docs、focused tests，并完成 full validation / commit / push。|

## Data Quality Gate

2382 只读取 prior validated artifacts，不读取 fresh cached market data、不重新 backtest、不生成
technical features、scoring、daily report 或交易建议，因此 `aits validate-data` 不适用。输出必须披露：

```text
data_quality_gate_executed=false
data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_RETEST_PLAN_ONLY_NO_FRESH_MARKET_DATA
```

如果实现范围变更为读取 fresh cached market data、运行 backtest 或生成 strategy state，则必须先运行：

```text
aits validate-data --as-of 2026-07-05
```

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_ranking_top_guarded_turnover_retest_plan/retest_plan_result.json
outputs/research_strategies/dynamic_strategy_ranking_top_guarded_turnover_retest_plan/ranking_top_fragility_diagnosis.json
outputs/research_strategies/dynamic_strategy_ranking_top_guarded_turnover_retest_plan/guarded_variant_plan.json
outputs/research_strategies/dynamic_strategy_ranking_top_guarded_turnover_retest_plan/variant_evaluation_plan.json
docs/research/dynamic_strategy_ranking_top_guarded_turnover_retest_plan.md
docs/research/dynamic_strategy_ranking_top_fragility_diagnosis.md
docs/research/dynamic_strategy_guarded_ranking_top_variant_plan.md
docs/research/dynamic_strategy_2383_route.md
```

## 验收标准

- CLI 返回 `DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY`。
- `next_direction_from_2381=OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`。
- `ranking_top_candidate=equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`。
- guardrail references 包含 `dynamic_regime_overlay_v0_4_lower_turnover` 和
  `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`。
- `primary_execution_cadence=valid_until_window`。
- `retest_plan_ready=true`。
- `ranking_top_fragility_diagnosis_ready=true`。
- `guarded_variant_plan_ready=true`。
- `variant_evaluation_plan_ready=true`。
- planned variants 覆盖 guarded turnover / cooldown / risk cap / valid-until decay / fusion /
  conservative fusion。
- forbidden optimization paths 明确禁止 monthly rebalance primary、stale signal carry-forward、
  ignoring costs/slippage、return-only optimization、paper-shadow / scheduler / broker dependency。
- scheduler、event append、outcome binding、paper-shadow、paper trade、shadow position、
  production、broker、daily report 全部保持 false / none。

## 进展记录

- 2026-07-06: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 retest plan，不是实际
  guarded variant retest、backtest、signal generation、observation approval、paper-shadow approval、
  scheduler enablement、production 或 broker readiness。
- 2026-07-06: 实现完成并归档 `DONE`。真实 run status=
  `DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY`，
  ranking top candidate=`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`，
  planned guarded variants=6，primary execution cadence=`valid_until_window`，monthly rebalance
  不得作为 primary decision，next route=
  `TRADING-2383_Dynamic_Strategy_Ranking_Top_Guarded_Variant_Retest`。本任务未重跑
  `aits validate-data`，因为只读取 prior validated TRADING-2365 / 2366 / 2379 / 2380 /
  2381 artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical features、
  scoring、daily report 或交易建议。scheduler、event append、outcome binding、paper-shadow、
  paper trade、shadow position、production、broker/order 和 daily report 全部保持 disabled /
  false / none。

## 验证记录

- `python -m ruff check .`：PASS。
- `python -m compileall -q src tests`：PASS。
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_ranking_top_guarded_turnover_retest_plan.py`：3 passed。
- `python -m ai_trading_system.cli research strategies dynamic-strategy-ranking-top-guarded-turnover-retest-plan --as-of 2026-07-06`：READY。
- `python -m ai_trading_system.cli docs validate-freshness`：567 docs PASS。
- `python -m ai_trading_system.cli docs report-contract --latest`：1279 reports PASS。
- `python -m ai_trading_system.cli reports task-register-consistency run`：
  active=319 / completed=441 / failed=0。
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：
  checks=5 / failed=0 / warnings=0。
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：
  197 passed；runtime artifact：
  `outputs/validation_runtime/contract-validation_20260705T154141Z/test_runtime_summary.json`。
