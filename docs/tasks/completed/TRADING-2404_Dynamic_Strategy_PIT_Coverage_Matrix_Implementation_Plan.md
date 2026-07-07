# TRADING-2404 Dynamic Strategy PIT Coverage Matrix Implementation Plan

最后更新：2026-07-07

## 状态

- task register id：`TRADING-2404_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN`
- status：`DONE`
- next route：`TRADING-2405_Dynamic_Strategy_PIT_Coverage_Matrix_Reusable_Implementation`

## 结论

TRADING-2404 将 TRADING-2403 的一次性 PIT coverage matrix / remediation review
固化为可复用基础设施实现计划。当前 blocking gaps 仍为 `growth_tilt_engine`
和 `valid_until_window`；它们继续阻断 candidate search、research-only
observation、paper-shadow 和 production。

本任务不实现完整 PIT matrix engine，不清除 blocking gaps，不运行新 backtest，不生成
新 signal/scoring，不批准 observation，不进入 paper-shadow / production / broker。
下一步限定为 TRADING-2405：实现 registry-backed reusable PIT coverage matrix
generator and gate checker。

## 输出

- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/implementation_plan_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/pit_input_registry_schema.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/pit_gate_policy.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/remediation_routes.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_implementation_plan/current_blocker_summary.json`
- `docs/research/dynamic_strategy_pit_coverage_matrix_implementation_plan.md`
- `docs/research/dynamic_strategy_pit_input_registry_schema.md`
- `docs/research/dynamic_strategy_pit_gate_policy.md`
- `docs/research/dynamic_strategy_pit_remediation_routes.md`
- `docs/research/dynamic_strategy_2405_route.md`

## Data Quality Gate

本任务未运行：

```text
python -m ai_trading_system.cli validate-data --as-of 2026-07-05
```

原因：

- 仅读取 prior validated TRADING-2403 / 2402 / 2401 artifacts。
- 不读取 fresh cached market data。
- 不生成 technical features、scoring、backtest、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_IMPLEMENTATION_PLAN_ONLY_NO_FRESH_MARKET_DATA`

## 安全边界

- `candidate_search_allowed=false`
- `research_only_observation_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
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

- `python -m ruff check src\ai_trading_system\dynamic_strategy_pit_coverage_matrix_implementation_plan.py tests\research_strategies\test_dynamic_strategy_pit_coverage_matrix_implementation_plan.py src\ai_trading_system\cli_commands\research_execution_semantics.py`：PASS
- `python -m py_compile src\ai_trading_system\dynamic_strategy_pit_coverage_matrix_implementation_plan.py tests\research_strategies\test_dynamic_strategy_pit_coverage_matrix_implementation_plan.py src\ai_trading_system\cli_commands\research_execution_semantics.py`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_pit_coverage_matrix_implementation_plan.py`：3 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-pit-coverage-matrix-implementation-plan --as-of 2026-07-07`：PASS，status=`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=591，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：PASS，reports=1301，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，active=319，completed=465，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T083347Z/test_runtime_summary.json`
