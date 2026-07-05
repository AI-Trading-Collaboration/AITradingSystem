# TRADING-2386 Dynamic Strategy Expanded Candidate Pool Retest And Screening

最后更新：2026-07-06

## 结果

- 新增 `aits research strategies dynamic-strategy-expanded-candidate-pool-retest`。
- 新增 `src/ai_trading_system/dynamic_strategy_expanded_candidate_pool_retest.py`。
- 新增 focused tests：`tests/research_strategies/test_dynamic_strategy_expanded_candidate_pool_retest.py`。
- 生成 `expanded_candidate_retest_result.json`、`expanded_candidate_ranking.json`、`signal_family_screening.json`、`time_regime_slice_matrix.json`、`decision_update.json`。
- 生成 `docs/research/dynamic_strategy_expanded_candidate_pool_retest.md`、`dynamic_strategy_expanded_candidate_ranking.md`、`dynamic_strategy_signal_family_screening.md`、`dynamic_strategy_2387_route.md`。
- 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/task_register.md`、`docs/task_register_completed.md`。

## 真实运行结论

- status：`DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY`
- data quality：`PASS_WITH_WARNINGS` / errors=0 / warnings=2
- reference candidates：5
- new candidates tested：12
- signal families tested：6
- primary execution cadence：`valid_until_window`
- monthly rebalance primary decision：`false`
- best candidate：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- best decision：`CONTINUE_OPTIMIZATION`
- candidate ready for research-only observation：`false`
- next route：`TRADING-2387_Dynamic_Strategy_Expanded_Candidate_Owner_Review_And_Next_Research_Decision`

## 安全边界

2386 是 strategy research actual retest / screening，不是 observation approval、
paper-shadow approval、scheduler enablement、daily report、production 或 broker
readiness。

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

已执行：

```bash
python -m ai_trading_system.cli validate-data --as-of 2026-07-05
```

结果：

- status：`PASS_WITH_WARNINGS`
- errors：0
- warnings：2
- quality report：`outputs/reports/data_quality_2026-07-05.md`
- validation audit：`artifacts/data_refresh_audit/validation/validate_data_2026-07-05_10c123e49e8eedfc.json`

## 验证

- `python -m ai_trading_system.cli validate-data --as-of 2026-07-05`：`PASS_WITH_WARNINGS` / errors=0 / warnings=2
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_expanded_candidate_pool_retest.py`：3 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-expanded-candidate-pool-retest --as-of 2026-07-05`：READY
- `python -m ai_trading_system.cli docs validate-freshness`：571 docs PASS
- `python -m ai_trading_system.cli docs report-contract --latest`：1283 reports PASS
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-06`：active=319 / completed=445 / failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --as-of 2026-07-06`：checks=5 / failed=0 / warnings=0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed
- runtime artifact：`outputs/validation_runtime/contract-validation_20260705T170213Z/test_runtime_summary.json`
- `git diff --check`：PASS，仅 CRLF normalization warning
