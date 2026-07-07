# TRADING-2402 Dynamic Strategy Data PIT And Signal Quality Gap Review

最后更新：2026-07-07

## 状态

- task register id：`TRADING-2402_DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW`
- status：`DONE`
- next route：`TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review`

## 结论

TRADING-2402 确认 TRADING-2401 后不应恢复局部 candidate search。当前瓶颈更可能来自
data quality caveat、PIT coverage、signal construction、valid-until / stale-signal
证据、regime labeling 和 threshold meta-dataset，而不是缺少更多 recombination variants。

默认下一步为 TRADING-2403，优先构建 PIT coverage matrix 并复核 signal construction
framework；同时建议 regime expectation scoring review 和 threshold meta-dataset。

## 输出

- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/gap_review_result.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/data_quality_gap_matrix.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/pit_coverage_gap_review.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/signal_quality_gap_review.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/regime_labeling_gap_review.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/threshold_meta_dataset_gap_review.json`
- `docs/research/dynamic_strategy_data_pit_signal_quality_gap_review.md`
- `docs/research/dynamic_strategy_data_quality_gap_matrix.md`
- `docs/research/dynamic_strategy_pit_coverage_gap_review.md`
- `docs/research/dynamic_strategy_signal_quality_gap_review.md`
- `docs/research/dynamic_strategy_2403_route.md`

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
- audit artifact：`artifacts/data_refresh_audit/validation/validate_data_2026-07-05_63e4bc4b675972a7.json`

warning classification：

- `prices_download_manifest_checksum_missing`：dynamic strategy relevance=`MATERIAL`；影响缓存 provenance / auditability，不直接改变 ranking math，但后续解释必须披露。
- `prices_adjustment_ratio_jump`：dynamic strategy relevance=`MATERIAL`；TQQQ 属于 dynamic strategy universe，需复核 corporate action / adjusted close ratio。

## 安全边界

- `candidate_auto_accept_approved=false`
- `research_only_observation_approved=false`
- `resume_candidate_search_recommended=false`
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

- `python -m ruff check src\ai_trading_system\dynamic_strategy_data_pit_signal_quality_gap_review.py tests\research_strategies\test_dynamic_strategy_data_pit_signal_quality_gap_review.py src\ai_trading_system\cli_commands\research_execution_semantics.py`：PASS
- `python -m py_compile src\ai_trading_system\dynamic_strategy_data_pit_signal_quality_gap_review.py tests\research_strategies\test_dynamic_strategy_data_pit_signal_quality_gap_review.py src\ai_trading_system\cli_commands\research_execution_semantics.py`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_data_pit_signal_quality_gap_review.py`：3 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-data-pit-signal-quality-gap-review --as-of 2026-07-07 --validate-data-as-of 2026-07-05`：PASS，status=`DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=589，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：PASS，reports=1299，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，active=319，completed=463，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T063644Z/test_runtime_summary.json`
- `git diff --check`：PASS
