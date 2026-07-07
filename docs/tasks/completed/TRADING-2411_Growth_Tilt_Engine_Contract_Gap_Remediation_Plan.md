# TRADING-2411 Growth Tilt Engine Contract Gap Remediation Plan

最后更新：2026-07-08

## 状态

- task register id：`TRADING-2411_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN`
- status：`DONE`
- next route：`TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation`

## 结论

TRADING-2411 将 TRADING-2410 中 `growth_tilt_engine` 的 7 个 blocked/gap source features 转成 remediation plan、deterministic implementation ordering 和 validation design。真实 CLI run 返回 `GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED`，`gap_count=7`、`unclassified_remediation_item_count=0`、`silent_gap_resolution_count=0`、`silent_blocker_downgrade_count=0`。

当前 `growth_tilt_engine` 与 `valid_until_window` blocker 保持 unresolved / undowngraded。本任务没有执行 remediation、没有修改 `growth_tilt_engine` 评分逻辑、没有生成 feature / signal / scoring / backtest / daily report、没有运行 candidate search，也没有进入 observation / paper-shadow / production / broker 路径。

## Remediation Ordering

真实 ordered remediation items：

- `1` / `equal_risk_baseline_weights` / `source_traceability_required`
- `2` / `target_vol_policy` / `source_traceability_required`
- `3` / `trend_features` / `source_traceability_required`
- `4` / `drawdown_features` / `as_of_semantics_required`
- `5` / `volatility_inputs` / `as_of_semantics_required`
- `6` / `execution_signal_validity_policy` / `validity_dependency_required`
- `7` / `growth_tilt_engine_signal_artifact` / `blocked_pending_prior_remediation`

每个 item 均输出 `remediation_order`、`blocks_contract_ready=true`、`blocks_pit_gate=true`、`requires_owner_review`、`can_be_implemented_without_fresh_market_data=true`、remediation action、required upstream artifact、required code/doc/config change、validation requirement、dependency ordering 和 blocker impact。

## 输出

- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/remediation_plan_result.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/contract_gap_remediation_plan.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/ordered_remediation_items.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/validation_design.json`
- `outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/unresolved_blocker_summary.json`
- `docs/research/growth_tilt_engine_contract_gap_remediation_plan.md`
- `docs/research/growth_tilt_engine_contract_gap_validation_design.md`
- `docs/research/dynamic_strategy_2412_route.md`

## Data Quality Gate

本任务未运行：

```text
python -m ai_trading_system.cli validate-data
```

原因：

- 仅读取 TRADING-2410 prior mapping artifacts、research docs、report registry 和 artifact catalog。
- 不读取 fresh cached market data。
- 不生成 technical features、trading signal、scoring、backtest、daily report 或交易建议。

输出披露：

- `data_quality_gate_executed=false`
- `data_quality_gate_reason=NOT_APPLICABLE_CONTRACT_GAP_REMEDIATION_PLAN_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

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

- `python -m pytest -n 16 --dist loadfile tests/research_quality/test_growth_tilt_engine_gap_remediation.py tests/research_strategies/test_growth_tilt_engine_contract_gap_remediation_plan.py -q`：8 passed
- `aits research strategies growth-tilt-engine-contract-gap-remediation-plan --as-of 2026-07-08`：PASS，status=`GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED`
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m ai_trading_system.cli docs validate-freshness`：PASS，检查文档数=598，问题数=0
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-08`：PASS，reports=1308，errors=0，warnings=0
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-08`：PASS，active=319 / completed=472，failed=0
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0
- `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs\task_register.md`：无匹配，exit=1 符合预期
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260707T161349Z/test_runtime_summary.json`
- `git diff --check`：PASS，仅提示 `docs/task_register.md` 与 `src/ai_trading_system/cli_commands/research_execution_semantics.py` 工作区行尾会在下次 Git 写入时标准化。
