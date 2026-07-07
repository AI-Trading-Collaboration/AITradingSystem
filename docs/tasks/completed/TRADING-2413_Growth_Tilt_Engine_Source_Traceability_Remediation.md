# TRADING-2413 Growth Tilt Engine Source Traceability Remediation

最后更新：2026-07-08

## 状态

- task register id：`TRADING-2413_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION`
- status：`DONE`
- next route：`TRADING-2414_Growth_Tilt_Engine_Signal_Validity_Dependency_Remediation`

## 结论

TRADING-2413 将 TRADING-2411 / 2412 中 source traceability 相关的 7 个 `growth_tilt_engine` source features 转成 explicit traceability metadata、before/after mapping 和 remaining blocker summary。真实 CLI run 返回 `GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`。

本任务只补 source traceability 维度。`equal_risk_baseline_weights` 和 `risk_on_trend_filter_context` 已基于 governed config file + key + SHA256 snapshot 完成 traceability remediation。`target_vol_policy`、`trend_features`、`volatility_inputs`、`drawdown_features` 和 `growth_tilt_engine_signal_artifact` 因缺少可确认 upstream key / source snapshot / standalone artifact 明确保留 blocked。signal validity dependency、PIT gate evidence 和 `valid_until_window` blocker 仍未在 2413 修复，因此 `contract_ready_count=0`，`remaining_blocked_or_gap_count=7`，`growth_tilt_engine` 与 `valid_until_window` blocker 均保持 unresolved / undowngraded。

## Remediated Items

- `equal_risk_baseline_weights`：`source_traceability_remediated`
- `risk_on_trend_filter_context`：`source_traceability_remediated`

## Blocked Items

- `target_vol_policy`：`source_traceability_blocked_by_missing_upstream_artifact`
- `trend_features`：`source_traceability_blocked_by_missing_upstream_artifact`
- `volatility_inputs`：`source_traceability_blocked_by_missing_upstream_artifact`
- `drawdown_features`：`source_traceability_blocked_by_missing_upstream_artifact`
- `growth_tilt_engine_signal_artifact`：`source_traceability_blocked_by_missing_upstream_artifact`

## 输出

- `outputs/research_strategies/growth_tilt_engine_source_traceability_remediation/source_traceability_remediation_result.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_remediation/source_traceability_contract_metadata.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_remediation/before_after_source_traceability_remediation.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_remediation/updated_source_feature_mapping.json`
- `outputs/research_strategies/growth_tilt_engine_source_traceability_remediation/remaining_blocker_summary.json`
- `docs/research/growth_tilt_engine_source_traceability_remediation.md`
- `docs/research/growth_tilt_engine_source_traceability_contract_metadata.md`
- `docs/research/dynamic_strategy_2414_route.md`

## Data Quality Gate

本任务未运行：

```text
python -m ai_trading_system.cli validate-data
```

原因：

- 仅读取 TRADING-2412 / 2411 prior artifacts、research docs、report registry、artifact catalog 和 governed config。
- 不读取 fresh cached market data。
- 不生成 technical features、trading signal、scoring、backtest、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_SOURCE_TRACEABILITY_REMEDIATION_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## 安全边界

- `growth_tilt_engine_blocker_resolved=false`
- `growth_tilt_engine_blocker_downgraded=false`
- `valid_until_window_blocker_resolved=false`
- `valid_until_window_blocker_downgraded=false`
- `blockers_resolved=false`
- `blockers_downgraded=false`
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
- `new_feature_generated=false`
- `new_signal_generated=false`
- `new_strategy_backtest_run=false`
- `scoring_run=false`
- `production_enabled=false`
- `production_allowed=false`
- `production_effect=none`
- `broker_enabled=false`
- `broker_action=none`
- `broker_action_enabled=false`

## 验证记录

- `python -m pytest -n 16 --dist loadfile tests/research_quality/test_growth_tilt_engine_source_traceability_remediation.py tests/research_strategies/test_growth_tilt_engine_source_traceability_strategy_remediation.py -q`：8 passed
- `aits research strategies growth-tilt-engine-source-traceability-remediation --as-of 2026-07-08`：PASS，status=`GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=600，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-08`：PASS，reports=1310，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-08`：PASS，active=319 / completed=474，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T165306Z/test_runtime_summary.json`
