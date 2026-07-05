# TRADING-2381 Dynamic Strategy Optimization Plateau And Next Candidate Decision

最后更新：2026-07-06

## 背景

TRADING-2380 已完成 optimized variant owner review and observation decision，真实状态为
`DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`。

2380 明确不批准 research-only observation / paper-shadow / execution chain，并把下一步固定为
`TRADING-2381_Dynamic_Strategy_Optimization_Plateau_And_Next_Candidate_Decision`。

TRADING-2379 中 `dynamic_regime_overlay_v0_4_cooldown_balanced_v1` 是当前 lower-turnover
优化线的 best variant，但 best decision 仍为 `CONTINUE_OPTIMIZATION`，time/regime slice
robustness 仍不足，并且相对 ranking top 仍存在 return gap。

## 范围

本任务新增
`aits research strategies dynamic-strategy-optimization-plateau-next-candidate-decision`，
只读取 TRADING-2365 / 2366 / 2376 / 2379 / 2380 prior artifacts，不重新 backtest、不读取 fresh
cached market data、不生成新信号。任务目标是判断当前 lower-turnover 优化线是否进入局部
optimization plateau，并选择下一阶段候选方向。

默认 next direction：

```text
OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT
```

推荐 next route：

```text
TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan
```

## 允许与禁止

允许：

- 读取既有 TRADING-2365 candidate ranking artifacts。
- 读取既有 TRADING-2366 sensitivity artifacts。
- 读取既有 TRADING-2376 targeted retest artifacts。
- 读取既有 TRADING-2379 optimized variant retest artifacts。
- 读取既有 TRADING-2380 owner review / observation rejection artifacts。
- 评估 optimization plateau。
- 生成 owner / research decision 和 next candidate direction。

禁止：

- 运行新 backtest 或生成新 signal。
- 启用 scheduler、创建 scheduled task 或生成 daily report。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 进入 production、调用 broker API 或生成 order。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. Source registration and validation|DONE|登记任务；fail-closed 校验 2365 / 2366 / 2376 / 2379 / 2380 source status、2380 route、2379 best variant / decision、primary cadence 和 safety fields。|
|2. Plateau assessment|DONE|生成 lower-turnover optimization review、plateau evidence、blocker attribution 和 `optimization_plateau_detected`。|
|3. Next candidate direction|DONE|评估 A/B/C/D/E 选项，输出 `next_direction_decision`、recommended default direction 和 TRADING-2382 route。|
|4. Registry, docs and validation|DONE|更新 report registry、artifact catalog、system flow、task register、completed doc、research docs、focused tests，并完成 full validation / commit / push。|

## Data Quality Gate

2381 只读取 prior validated artifacts，不读取 fresh cached market data、不重新 backtest、不生成
technical features、scoring、daily report 或交易建议，因此 `aits validate-data` 不适用。
输出必须披露：

```text
data_quality_gate_executed=false
data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_PLATEAU_DECISION_ONLY_NO_FRESH_MARKET_DATA
```

## 必须输出

```text
outputs/research_strategies/dynamic_strategy_optimization_plateau_next_candidate_decision/optimization_plateau_decision.json
outputs/research_strategies/dynamic_strategy_optimization_plateau_next_candidate_decision/next_candidate_direction.json
docs/research/dynamic_strategy_optimization_plateau_next_candidate_decision.md
docs/research/dynamic_strategy_plateau_review.md
docs/research/dynamic_strategy_next_candidate_direction.md
docs/research/dynamic_strategy_2382_route.md
```

## 验收标准

- CLI 返回 `DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY`。
- `best_variant_from_2379=dynamic_regime_overlay_v0_4_cooldown_balanced_v1`。
- `best_variant_decision_from_2379=CONTINUE_OPTIMIZATION`。
- `observation_approved_from_2380=false`。
- `optimization_plateau_review_ready=true`。
- 输出 `optimization_plateau_detected`、`next_direction_decision` 和
  `recommended_default_direction`。
- 推荐 route 为 `TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan`。
- scheduler、event append、outcome binding、paper-shadow、paper trade、shadow position、
  production、broker、daily report 全部保持 false / none。

## 进展记录

- 2026-07-06: 根据 owner 附件新增并进入 `IN_PROGRESS`。本任务是 owner / research
  decision record，不是新 backtest、signal generation、observation approval、paper-shadow approval、
  scheduler enablement、production 或 broker readiness。
- 2026-07-06: 实现完成并归档 `DONE`。真实 run status=
  `DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY`，
  optimization plateau=`LOWER_TURNOVER_LOCAL_PLATEAU_DETECTED`，
  next direction=`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`，
  recommended default direction 同为 OPTION_B，next route=
  `TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan`。本任务未重跑
  `aits validate-data`，因为只读取 prior validated TRADING-2365 / 2366 / 2376 / 2379 /
  2380 artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical
  features、scoring、daily report 或交易建议。scheduler、event append、outcome binding、
  paper-shadow、paper trade、shadow position、production、broker/order 和 daily report 全部保持
  disabled / false / none。

## 验证记录

- `python -m ruff check .`：PASS。
- `python -m compileall -q src tests`：PASS。
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_optimization_plateau_next_candidate_decision.py`：3 passed。
- `python -m ai_trading_system.cli research strategies dynamic-strategy-optimization-plateau-next-candidate-decision --as-of 2026-07-06`：READY。
- `python -m ai_trading_system.cli docs validate-freshness`：566 docs PASS。
- `python -m ai_trading_system.cli docs report-contract --latest`：1278 reports PASS。
- `python -m ai_trading_system.cli reports task-register-consistency run`：
  active=319 / completed=440 / failed=0。
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：
  checks=5 / failed=0 / warnings=0。
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：
  197 passed；runtime artifact：
  `outputs/validation_runtime/contract-validation_20260705T152529Z/test_runtime_summary.json`。
