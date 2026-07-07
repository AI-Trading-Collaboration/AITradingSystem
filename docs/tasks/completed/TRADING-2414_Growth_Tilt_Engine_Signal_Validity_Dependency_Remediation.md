# TRADING-2414 Growth Tilt Engine Signal Validity Dependency Remediation

最后更新：2026-07-08

## 归档状态

- task register id：`TRADING-2414_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION`
- status：`DONE`
- priority：`P0`
- next route：`TRADING-2415_Growth_Tilt_Engine_PIT_Gate_Readiness_Snapshot`
- production effect：`none`
- broker action：`none`

## 完成摘要

TRADING-2414 将 TRADING-2411 / 2412 / 2413 后仍未评估的 `growth_tilt_engine` signal validity dependency 维度转成 explicit contract metadata、before/after mapping、updated source feature mapping 和 remaining blocker summary。真实 CLI run 返回 `GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`。

本任务没有实现 `valid_until_window`，没有修改 `growth_tilt_engine` 评分逻辑，没有生成 feature / signal / scoring / backtest / daily report，没有运行 candidate search，也没有进入 observation、paper-shadow、production 或 broker。

## 真实结果

- `input_gap_count=7`
- `validity_dependency_gap_count=8`
- `validity_dependency_remediated_count=2`
- `validity_dependency_blocked_by_valid_until_window_count=1`
- `validity_dependency_blocked_by_source_traceability_count=5`
- `remaining_blocked_or_gap_count=7`
- `contract_ready_count=0`
- `as_of_status_rollback_count=0`
- `source_traceability_status_rollback_count=0`
- `growth_tilt_engine_blocker_resolved=false`
- `growth_tilt_engine_blocker_downgraded=false`
- `valid_until_window_blocker_resolved=false`
- `valid_until_window_blocker_downgraded=false`
- `candidate_search_enabled=false`
- `observation_enabled=false`
- `paper_shadow_enabled=false`
- `production_enabled=false`
- `broker_enabled=false`

## Feature 分类

- `equal_risk_baseline_weights`：`validity_dependency_remediated`
- `risk_on_trend_filter_context`：`validity_dependency_remediated`
- `execution_signal_validity_policy`：`validity_dependency_blocked_by_valid_until_window`
- `target_vol_policy`：`validity_dependency_blocked_by_missing_source_traceability`
- `trend_features`：`validity_dependency_blocked_by_missing_source_traceability`
- `volatility_inputs`：`validity_dependency_blocked_by_missing_source_traceability`
- `drawdown_features`：`validity_dependency_blocked_by_missing_source_traceability`
- `growth_tilt_engine_signal_artifact`：`validity_dependency_blocked_by_missing_source_traceability`

## 输出

- `outputs/research_strategies/growth_tilt_engine_signal_validity_dependency_remediation/signal_validity_dependency_remediation_result.json`
- `outputs/research_strategies/growth_tilt_engine_signal_validity_dependency_remediation/signal_validity_dependency_contract_metadata.json`
- `outputs/research_strategies/growth_tilt_engine_signal_validity_dependency_remediation/before_after_signal_validity_dependency_remediation.json`
- `outputs/research_strategies/growth_tilt_engine_signal_validity_dependency_remediation/updated_source_feature_mapping.json`
- `outputs/research_strategies/growth_tilt_engine_signal_validity_dependency_remediation/remaining_blocker_summary.json`
- `docs/research/growth_tilt_engine_signal_validity_dependency_remediation.md`
- `docs/research/growth_tilt_engine_signal_validity_dependency_contract_metadata.md`
- `docs/research/dynamic_strategy_2415_route.md`

## Data Quality Boundary

本任务未运行 `aits validate-data`。原因：只读取 TRADING-2413 / 2412 / 2411 prior artifacts/docs/registry/catalog，不读取 fresh cached market data、不运行新 backtest、不生成 feature/signal/scoring/daily report 或交易建议。

## Safety Boundary

- candidate search：disabled
- observation：disabled
- paper-shadow：disabled
- scheduler / event append / outcome binding：disabled
- production：disabled
- broker / order：disabled
- `growth_tilt_engine` blocker：unresolved / undowngraded
- `valid_until_window` blocker：unresolved / undowngraded

## 验证记录

- `python -m pytest -n 16 --dist loadfile tests\research_quality\test_growth_tilt_engine_signal_validity_dependency_remediation.py tests\research_strategies\test_growth_tilt_engine_signal_validity_dependency_strategy.py -q`：PASS，8 passed
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `aits research strategies growth-tilt-engine-signal-validity-dependency-remediation --as-of 2026-07-08`：PASS，status=`GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，601 docs，0 issues
- `python -m ai_trading_system.cli docs report-contract`：PASS，1311 reports，0 errors，0 warnings
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-08`：PASS，active=319，completed=475，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，active register 终态行检查通过
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T171506Z/test_runtime_summary.json`
- `git diff --check`：PASS，退出码 0；仅提示 `docs/task_register.md` 和 `src/ai_trading_system/cli_commands/research_execution_semantics.py` 的 CRLF normalization warning
