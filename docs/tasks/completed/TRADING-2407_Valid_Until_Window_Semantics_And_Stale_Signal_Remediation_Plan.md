# TRADING-2407 Valid-Until Window Semantics And Stale Signal Remediation Plan

最后更新：2026-07-07

## 状态

- task register id：`TRADING-2407_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN`
- status：`DONE`
- next route：`TRADING-2408_Dynamic_Strategy_Blocking_Gap_Remediation_Implementation_Plan`

## 结论

TRADING-2407 将 TRADING-2405 的 `valid_until_window` blocking gap 转成可审计 remediation plan。当前结论保持 fail-closed：`valid_until_window` 仍是 `BLOCKING`，PIT status 仍为 `UNKNOWN_OR_APPROXIMATE_PIT`，本任务没有清除或降级 blocker，也没有恢复 candidate search。

本任务生成了 valid-until semantics review、stale signal risk audit、signal validity contract plan、growth tilt alignment review、remediation plan、severity downgrade conditions 和 validation plan。真实 CLI run 返回 `DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN_READY`，semantics review 6 行，stale signal risk audit 8 项。下一步限定为 TRADING-2408：统一设计 blocking gap remediation implementation sequence。

## 输出

- `outputs/research_strategies/dynamic_strategy_valid_until_window_stale_signal_remediation_plan/remediation_plan_result.json`
- `outputs/research_strategies/dynamic_strategy_valid_until_window_stale_signal_remediation_plan/valid_until_semantics_review.json`
- `outputs/research_strategies/dynamic_strategy_valid_until_window_stale_signal_remediation_plan/stale_signal_risk_audit.json`
- `outputs/research_strategies/dynamic_strategy_valid_until_window_stale_signal_remediation_plan/signal_validity_contract_plan.json`
- `outputs/research_strategies/dynamic_strategy_valid_until_window_stale_signal_remediation_plan/severity_downgrade_conditions.json`
- `outputs/research_strategies/dynamic_strategy_valid_until_window_stale_signal_remediation_plan/validation_plan.json`
- `docs/research/dynamic_strategy_valid_until_window_stale_signal_remediation_plan.md`
- `docs/research/dynamic_strategy_valid_until_semantics_review.md`
- `docs/research/dynamic_strategy_stale_signal_risk_audit.md`
- `docs/research/dynamic_strategy_signal_validity_contract.md`
- `docs/research/dynamic_strategy_2408_route.md`

## Data Quality Gate

本任务未运行：

```text
python -m ai_trading_system.cli validate-data --as-of 2026-07-05
```

原因：

- 仅读取 prior validated TRADING-2405 / 2406 / 2403 artifacts。
- 读取 governed PIT input registry、strategy execution policy registry 和 signal validity taxonomy。
- 不读取 fresh cached market data。
- 不生成 technical features、scoring、backtest、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_CONFIG_ONLY_NO_FRESH_MARKET_DATA`

## 安全边界

- `valid_until_window_blocking_gap_resolved=false`
- `valid_until_window_severity_downgraded=false`
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

- `python -m ruff check src\ai_trading_system\dynamic_strategy_valid_until_window_stale_signal_remediation_plan.py src\ai_trading_system\cli_commands\research_execution_semantics.py tests\research_strategies\test_dynamic_strategy_valid_until_window_stale_signal_remediation_plan.py`：PASS
- `python -m compileall -q src\ai_trading_system\dynamic_strategy_valid_until_window_stale_signal_remediation_plan.py tests\research_strategies\test_dynamic_strategy_valid_until_window_stale_signal_remediation_plan.py`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_valid_until_window_stale_signal_remediation_plan.py`：3 passed
- `python -m ai_trading_system.cli research strategies dynamic-strategy-valid-until-window-stale-signal-remediation-plan --as-of 2026-07-07`：PASS，status=`DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN_READY`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=594，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：PASS，reports=1304，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，归档前 active=320 / completed=467，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T125823Z/test_runtime_summary.json`
- 归档后 `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=594，问题数=0
- 归档后 `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：PASS，reports=1304，errors=0，warnings=0
- 归档后 `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，active=319 / completed=468，failed=0
- 归档后 `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- 归档后 `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
