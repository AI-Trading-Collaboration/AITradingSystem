# TRADING-2403 Dynamic Strategy PIT Coverage Matrix And Signal Construction Review

最后更新：2026-07-07

## 状态

- task register id：`TRADING-2403_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW`
- status：`DONE`
- next route：`TRADING-2404_Dynamic_Strategy_PIT_Coverage_Matrix_Implementation_Plan`

## 结论

TRADING-2403 将 TRADING-2402 的 data / PIT / signal quality gap review 落成逐项
PIT coverage matrix 和 signal construction review。当前动态策略 inputs 中，market
prices 可作为 true PIT 输入继续披露使用；adjusted prices、returns、volatility /
trend / drawdown features、lower-turnover guardrail、stale-signal detection 和
regime labels 主要是 approximate PIT；`growth_tilt_engine` 和 `valid_until_window`
仍是 blocking gap。

本任务不恢复 candidate search，不批准 observation，不进入 paper-shadow / production /
broker。下一步限定为 TRADING-2404：把 PIT coverage matrix 固化为可复用工具和报告。

## 输出

- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/pit_signal_review_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/pit_coverage_matrix.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/signal_construction_review.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/regime_labeling_review.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/remediation_matrix.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/threshold_meta_dataset_gap.json`
- `docs/research/dynamic_strategy_pit_coverage_signal_construction_review.md`
- `docs/research/dynamic_strategy_pit_coverage_matrix.md`
- `docs/research/dynamic_strategy_signal_construction_review.md`
- `docs/research/dynamic_strategy_regime_labeling_review.md`
- `docs/research/dynamic_strategy_2404_route.md`

## Data Quality Gate

本任务运行：

```text
python -m ai_trading_system.cli validate-data --as-of 2026-07-05
```

结果：

- status：`PASS_WITH_WARNINGS`
- errors：0
- warnings：2
- report：`outputs/reports/data_quality_2026-07-05.md`
- audit artifact：`artifacts/data_refresh_audit/validation/validate_data_2026-07-05_becbcfd2e105dee4.json`

warnings：

- `prices_download_manifest_checksum_missing`：material cache provenance / auditability caveat。
- `prices_adjustment_ratio_jump`：material TQQQ adjusted-price interpretation caveat。

## 安全边界

- `candidate_search_resumed=false`
- `candidate_auto_accept_approved=false`
- `research_only_observation_approved=false`
- `new_strategy_backtest_run=false`
- `new_signal_generated=false`
- `scoring_run=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `daily_report_generated=false`
- `production_effect=none`
- `production_enabled=false`
- `broker_action=none`
- `broker_action_enabled=false`

## 验证记录

- `python -m ruff check src\ai_trading_system\dynamic_strategy_pit_coverage_signal_construction_review.py tests\research_strategies\test_dynamic_strategy_pit_coverage_signal_construction_review.py src\ai_trading_system\cli_commands\research_execution_semantics.py`：PASS
- `python -m py_compile src\ai_trading_system\dynamic_strategy_pit_coverage_signal_construction_review.py tests\research_strategies\test_dynamic_strategy_pit_coverage_signal_construction_review.py src\ai_trading_system\cli_commands\research_execution_semantics.py`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_pit_coverage_signal_construction_review.py`：3 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-pit-coverage-signal-construction-review --as-of 2026-07-07 --validate-data-as-of 2026-07-05`：PASS，status=`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_READY`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=590，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：PASS，reports=1300，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，active=319，completed=464，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T070000Z/test_runtime_summary.json`
- `git diff --check`：PASS
