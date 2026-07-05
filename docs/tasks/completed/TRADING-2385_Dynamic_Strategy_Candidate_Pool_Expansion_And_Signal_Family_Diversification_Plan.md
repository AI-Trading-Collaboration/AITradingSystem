# TRADING-2385 Dynamic Strategy Candidate Pool Expansion And Signal Family Diversification Plan

最后更新：2026-07-06

## 结果

- 新增 `aits research strategies dynamic-strategy-candidate-pool-expansion-plan`。
- 新增 `src/ai_trading_system/dynamic_strategy_candidate_pool_expansion_plan.py`。
- 新增 focused tests：`tests/research_strategies/test_dynamic_strategy_candidate_pool_expansion_plan.py`。
- 生成 `candidate_pool_expansion_plan.json`、`signal_family_diversification_plan.json`、`candidate_budget_guardrails.json`、`retest_plan_2386.json`。
- 生成 `docs/research/dynamic_strategy_candidate_pool_expansion_plan.md`、`dynamic_strategy_signal_family_diversification_plan.md`、`dynamic_strategy_candidate_budget_and_anti_overfit_guardrails.md`、`dynamic_strategy_2386_route.md`。
- 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/task_register.md`、`docs/task_register_completed.md`。

## 真实运行结论

- status：`DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN_READY`
- owner decision from 2384：`DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED`
- reference candidates：5
- signal families：6
- default new candidates for 2386：12
- candidate_budget_ready：`true`
- anti_overfit_guardrails_ready：`true`
- retest_plan_2386_ready：`true`
- primary execution cadence：`valid_until_window`
- monthly rebalance primary decision：`false`
- next route：`TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening`

## 安全边界

2385 是 plan-only artifact，不是 expanded candidate retest、新 backtest、新 signal、
research-only observation approval、paper-shadow approval、scheduler enablement、
daily report、production 或 broker readiness。

以下全部保持 disabled / false / none：

- scheduler
- event append
- outcome binding
- paper-shadow
- paper trade
- shadow position
- production
- broker / order
- daily report

## Data Quality Gate

未重跑 `aits validate-data`。

原因：本任务只读取 prior validated TRADING-2365 / 2366 / 2379 / 2383 / 2384
artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical
features、scoring、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA`

## 验证

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_candidate_pool_expansion_plan.py`：3 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-candidate-pool-expansion-plan --as-of 2026-07-06`：READY
- `python -m ai_trading_system.cli docs validate-freshness`：570 docs PASS
- `python -m ai_trading_system.cli docs report-contract --latest`：1282 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=444 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --as-of 2026-07-06`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed
- runtime artifact：`outputs/validation_runtime/contract-validation_20260705T163743Z/test_runtime_summary.json`
- `git diff --check`：PASS，仅 CRLF normalization warning
