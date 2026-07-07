# TRADING-2410 Growth Tilt Engine Source Feature Contract Mapping

最后更新：2026-07-08

## 状态

- task register id：`TRADING-2410_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING`
- status：`DONE`
- next route：`TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan`

## 结论

TRADING-2410 为 `growth_tilt_engine` 建立了 source feature -> 2409 contract requirement mapping。真实 CLI run 返回 `GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED`，枚举 10 个已知 source features，`unclassified_feature_count=0`，所有 feature 都落入 allowed mapping status。

当前 blocker 保持 unresolved / undowngraded。本任务没有修复 growth tilt engine、没有标记 TRUE_PIT、没有生成 dynamic strategy signal、没有执行 candidate search / replay validation / backtest / scoring，也没有进入 observation / paper-shadow / production / broker 路径。

## Mapping 摘要

- known source features：10
- contract-ready features：0
- blocked or gap features：7
- mapped with caveats：3
- next route：`TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan`

真实 mapping statuses：

- `adjusted_prices`：`mapped_with_caveats`
- `returns`：`mapped_with_caveats`
- `volatility_inputs`：`missing_as_of_semantics`
- `trend_features`：`missing_source_traceability`
- `drawdown_features`：`missing_as_of_semantics`
- `equal_risk_baseline_weights`：`missing_source_traceability`
- `target_vol_policy`：`missing_source_traceability`
- `risk_on_trend_filter_context`：`mapped_with_caveats`
- `execution_signal_validity_policy`：`blocked_unresolved`
- `growth_tilt_engine_signal_artifact`：`blocked_unresolved`

## 输出

- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/mapping_result.json`
- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/source_feature_contract_mapping.json`
- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/contract_mapping_validation.json`
- `outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/unresolved_gap_summary.json`
- `docs/research/growth_tilt_engine_source_feature_contract_mapping.md`
- `docs/research/growth_tilt_engine_contract_mapping_validation.md`
- `docs/research/dynamic_strategy_2411_route.md`

## Data Quality Gate

本任务未运行：

```text
python -m ai_trading_system.cli validate-data
```

原因：

- 仅读取 prior validated TRADING-2409 / 2406 / 2405 artifacts。
- 读取 governed `config/research/dynamic_strategy_pit_input_registry.yaml` 与 `config/research/equal_risk_growth_tilt_candidate_registry.yaml`。
- 不读取 fresh cached market data。
- 不生成 technical features、trading signal、scoring、backtest、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_SOURCE_FEATURE_CONTRACT_MAPPING_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## 安全边界

- `blockers_resolved=false`
- `blockers_downgraded=false`
- `growth_tilt_engine_blocking_gap_resolved=false`
- `growth_tilt_engine_severity_downgraded=false`
- `candidate_search_enabled=false`
- `candidate_search_allowed=false`
- `candidate_search_resumed=false`
- `observation_enabled=false`
- `research_only_observation_allowed=false`
- `paper_shadow_enabled=false`
- `paper_shadow_allowed=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `daily_report_generated=false`
- `production_enabled=false`
- `production_allowed=false`
- `production_effect=none`
- `broker_enabled=false`
- `broker_action=none`
- `broker_action_enabled=false`

## 验证记录

- `python -m pytest -n 16 --dist loadfile tests/research_quality/test_growth_tilt_engine_contract_mapping.py tests/research_strategies/test_growth_tilt_engine_source_feature_contract_mapping.py -q`：9 passed
- `python -m ai_trading_system.cli research strategies growth-tilt-engine-source-feature-contract-mapping --as-of 2026-07-08`：PASS，status=`GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=597，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-08`：PASS，reports=1307，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-08`：PASS，归档前 active=320 / completed=470，failed=0
- `git diff --check`：PASS，仅提示 `docs/task_register.md` 与 `src/ai_trading_system/cli_commands/research_execution_semantics.py` 工作区行尾会在下次 Git 写入时标准化。
- 归档后 `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=597，问题数=0
- 归档后 `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-08`：PASS，reports=1307，errors=0，warnings=0
- 归档后 `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-08`：PASS，active=319 / completed=471，failed=0
- 归档后 `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- 归档后 `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T154744Z/test_runtime_summary.json`
