# TRADING-2409 Dynamic Strategy Signal As-Of And Validity Contract Schema

最后更新：2026-07-07

## 状态

- task register id：`TRADING-2409_DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA`
- status：`DONE`
- next route：`TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping`

## 结论

TRADING-2409 实现了 reusable signal contract schema 层：signal as-of contract、source feature traceability contract、signal validity contract、基础 schema validator、contract schema snapshot 和 PIT gate integration plan。当前结论保持 fail-closed：`growth_tilt_engine` 和 `valid_until_window` 都仍为 `BLOCKING`，本任务没有映射真实信号、没有执行 replay validation、没有清除 blocker、没有降级 severity、没有恢复 candidate search，也没有进入 observation / paper-shadow / production / broker 路径。

真实 CLI run 返回 `DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`。下一步限定为 TRADING-2410：把 `growth_tilt_engine` source features 映射到 source feature traceability contract 和 signal as-of contract。

## 输出

- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/contract_schema_result.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/signal_as_of_contract_schema.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/source_feature_traceability_contract_schema.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/signal_validity_contract_schema.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/contract_schema_snapshot.json`
- `outputs/research_strategies/dynamic_strategy_signal_as_of_validity_contract_schema/pit_gate_integration_plan.json`
- `outputs/research_quality/signal_contracts/signal_as_of_contract_schema.json`
- `outputs/research_quality/signal_contracts/source_feature_traceability_contract_schema.json`
- `outputs/research_quality/signal_contracts/signal_validity_contract_schema.json`
- `outputs/research_quality/signal_contracts/contract_schema_snapshot.json`
- `docs/research/dynamic_strategy_signal_as_of_validity_contract_schema.md`
- `docs/research/dynamic_strategy_signal_as_of_contract_schema.md`
- `docs/research/dynamic_strategy_source_feature_traceability_contract_schema.md`
- `docs/research/dynamic_strategy_signal_validity_contract_schema.md`
- `docs/research/dynamic_strategy_2410_route.md`

## Data Quality Gate

本任务未运行：

```text
python -m ai_trading_system.cli validate-data --as-of 2026-07-05
```

原因：

- 仅读取 prior validated TRADING-2408 / 2405 artifacts。
- 读取 governed `config/research/dynamic_strategy_pit_input_registry.yaml`。
- 不读取 fresh cached market data。
- 不生成 technical features、trading signal、scoring、backtest、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_SCHEMA_VALIDATOR_AND_PRIOR_VALIDATED_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## 安全边界

- `growth_tilt_engine_blocking_gap_resolved=false`
- `valid_until_window_blocking_gap_resolved=false`
- `any_blocker_severity_downgraded=false`
- `candidate_search_allowed=false`
- `candidate_search_resumed=false`
- `research_only_observation_allowed=false`
- `research_only_observation_approved=false`
- `paper_shadow_allowed=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `daily_report_generated=false`
- `production_allowed=false`
- `production_effect=none`
- `production_enabled=false`
- `broker_action=none`
- `broker_action_enabled=false`

## 验证记录

- `python -m ruff check src\ai_trading_system\research_quality\signal_as_of_contract.py src\ai_trading_system\research_quality\source_feature_traceability_contract.py src\ai_trading_system\research_quality\signal_validity_contract.py src\ai_trading_system\research_quality\signal_contract_schema_snapshot.py src\ai_trading_system\dynamic_strategy_signal_as_of_validity_contract_schema.py src\ai_trading_system\cli_commands\research_execution_semantics.py tests\research_quality\test_signal_as_of_contract.py tests\research_quality\test_source_feature_traceability_contract.py tests\research_quality\test_signal_validity_contract.py tests\research_strategies\test_dynamic_strategy_signal_as_of_validity_contract_schema.py`：PASS
- `python -m compileall -q src\ai_trading_system\research_quality\signal_as_of_contract.py src\ai_trading_system\research_quality\source_feature_traceability_contract.py src\ai_trading_system\research_quality\signal_validity_contract.py src\ai_trading_system\research_quality\signal_contract_schema_snapshot.py src\ai_trading_system\dynamic_strategy_signal_as_of_validity_contract_schema.py tests\research_quality\test_signal_as_of_contract.py tests\research_quality\test_source_feature_traceability_contract.py tests\research_quality\test_signal_validity_contract.py tests\research_strategies\test_dynamic_strategy_signal_as_of_validity_contract_schema.py`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_quality\test_signal_as_of_contract.py tests\research_quality\test_source_feature_traceability_contract.py tests\research_quality\test_signal_validity_contract.py tests\research_strategies\test_dynamic_strategy_signal_as_of_validity_contract_schema.py`：12 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-signal-as-of-validity-contract-schema --as-of 2026-07-07`：PASS，status=`DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=596，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：PASS，reports=1306，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，归档前 active=320 / completed=469，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T144414Z/test_runtime_summary.json`
- `git diff --check`：PASS，仅提示 `docs/task_register.md` 与 `src/ai_trading_system/cli_commands/research_execution_semantics.py` 工作区行尾会在下次 Git 写入时标准化。
- 归档后 `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=596，问题数=0
- 归档后 `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：PASS，reports=1306，errors=0，warnings=0
- 归档后 `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，active=319 / completed=470，failed=0
- 归档后 `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- 归档后 `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
