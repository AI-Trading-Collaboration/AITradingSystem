# TRADING-2381 Dynamic Strategy Optimization Plateau And Next Candidate Decision

完成日期：2026-07-06

## 结论

TRADING-2381 已完成并归档 `DONE`。新增
`aits research strategies dynamic-strategy-optimization-plateau-next-candidate-decision`，
把 TRADING-2380 的 observation rejection / plateau review route 转成 optimization plateau
and next candidate decision record。

真实 run 结论：

- status：`DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY`
- optimization plateau：`LOWER_TURNOVER_LOCAL_PLATEAU_DETECTED`
- best variant from 2379：`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
- best variant decision from 2379：`CONTINUE_OPTIMIZATION`
- observation approved from 2380：`false`
- next direction：`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`
- recommended default direction：`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`
- next route：`TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan`

2381 判断 lower-turnover 优化线已经进入局部 plateau。当前线仍有研究价值，但不再继续把默认下一步放在 lower-turnover 变体挤压上；下一步应切到 ranking top guarded-turnover retest plan。

## 产物

- `outputs/research_strategies/dynamic_strategy_optimization_plateau_next_candidate_decision/optimization_plateau_decision.json`
- `outputs/research_strategies/dynamic_strategy_optimization_plateau_next_candidate_decision/next_candidate_direction.json`
- `docs/research/dynamic_strategy_optimization_plateau_next_candidate_decision.md`
- `docs/research/dynamic_strategy_plateau_review.md`
- `docs/research/dynamic_strategy_next_candidate_direction.md`
- `docs/research/dynamic_strategy_2382_route.md`
- `docs/requirements/TRADING-2381_Dynamic_Strategy_Optimization_Plateau_And_Next_Candidate_Decision.md`

## Data Quality Gate

本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2365 / 2366 /
2376 / 2379 / 2380 artifacts，不读取 fresh cached market data、不重新 backtest、不生成
technical features、scoring、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_PLATEAU_DECISION_ONLY_NO_FRESH_MARKET_DATA`

## 安全边界

- scheduler：disabled
- scheduled task：not created
- event append：disabled
- outcome binding：disabled
- outcome store mutation：false
- paper-shadow：disabled
- paper trade：not created
- shadow position：not created
- production：disabled
- broker action：none
- order generated：false
- daily report：not generated
- new backtest：not run
- new signal：not generated

## 验证

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_optimization_plateau_next_candidate_decision.py`：3 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-optimization-plateau-next-candidate-decision --as-of 2026-07-06`：READY
- `python -m ai_trading_system.cli docs validate-freshness`：566 docs PASS
- `python -m ai_trading_system.cli docs report-contract --latest`：1278 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run`：
  active=319 / completed=440 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：
  checks=5 / failed=0 / warnings=0
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：
  197 passed；runtime artifact：
  `outputs/validation_runtime/contract-validation_20260705T152529Z/test_runtime_summary.json`
