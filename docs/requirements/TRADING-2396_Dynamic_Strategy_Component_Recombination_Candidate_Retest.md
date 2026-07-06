# TRADING-2396 Dynamic Strategy Component Recombination Candidate Retest

最后更新：2026-07-07

## 状态

- 状态：`DONE`
- 任务登记：`TRADING-2396_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST`
- 优先级：P0
- 下一路由：`TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_Observation_Decision`
- 任务边界：actual retest / backtest research；不批准 observation、paper-shadow、scheduler、event append、outcome binding、production、broker 或 daily report。

## 背景

TRADING-2395 已完成 component recombination candidate plan，确认以 `growth_tilt_engine` 作为 return engine，将 `lower_turnover_guardrail`、`valid_until_window`、`no_stale_signal_carry_forward`、`turnover_budgeting` 和 `cooldown_balancing` 作为 guardrail layer，并保留 `guarded_turnover_transfer` 为 owner-review component。

TRADING-2396 的职责是实际运行 recombination candidate retest，判断组合候选是否能保留收益弹性，同时降低换手、成本、回撤和 stale signal 风险。

## 非目标与安全边界

本任务不得：

- approve research-only observation；
- enable scheduler / create scheduled task；
- append event / mutate historical event log；
- bind outcome / mutate outcome store；
- enable paper-shadow / create paper trade / shadow position；
- enable production / broker / order；
- generate daily report；
- use monthly rebalance as primary decision cadence。

允许读取 prior validated TRADING-2386 / 2393 / 2394 / 2395 artifacts 和 cached market data，在数据质量门禁通过后运行 research-only recombination retest。

## 实施步骤

1. 新增 `src/ai_trading_system/dynamic_strategy_component_recombination_candidate_retest.py`。
2. 新增 CLI：`aits research strategies dynamic-strategy-component-recombination-candidate-retest`。
3. Fail-closed 校验 2395 plan、2394 owner decision、2393 component ablation retest 和 2386 expanded candidate screening。
4. 执行 cached-data quality gate，默认 `as_of=2026-07-05` 验证命令必须在最终验证中记录。
5. 使用 `valid_until_window` 主口径测试 6 个 recombination candidates，并输出 cost / slice / regime / cadence evidence。
6. 生成 candidate ranking、component evidence matrix、decision update 和 TRADING-2397 route。
7. 更新 report registry、artifact catalog、system flow、task register 和 completed archive。
8. 新增 focused tests。

## 验收标准

真实 CLI run 必须返回：

- `status=DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY`
- `data_quality_gate_executed=true`
- `primary_execution_cadence=valid_until_window`
- `recombination_candidates_tested` 覆盖 6 个 TRADING-2395 planned candidates
- `reference_candidates` 覆盖 static baseline、raw growth tilt、lower turnover、cooldown balanced、guarded turnover
- `recombination_retest_ready=true`
- `candidate_ranking_ready=true`
- `component_evidence_matrix_ready=true`
- `decision_update_ready=true`
- `best_recombination_candidate` 存在
- `best_recombination_decision` 存在
- `recommended_next_research_task=TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_Observation_Decision`
- candidate auto-accept、research-only observation、paper-shadow、scheduler、event append、outcome binding、production、broker、daily report 全部为 false / none

## 数据质量门禁

本任务会运行 recombination candidate retest / backtest，必须运行：

```bash
aits validate-data --as-of 2026-07-05
```

CLI builder 也必须调用同源 data quality gate，并在输出中披露 `data_quality_status`、report path、row count、checksum 和 requested date range。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_component_recombination_candidate_retest.py`
- `python -m ai_trading_system.cli research strategies dynamic-strategy-component-recombination-candidate-retest --as-of 2026-07-07`
- `python -m ai_trading_system.cli validate-data --as-of 2026-07-05`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。本任务会运行 actual recombination retest / backtest，必须执行 cached-data quality gate；不批准 observation、paper-shadow、scheduler、event append、outcome binding、production、broker 或 daily report。
- 2026-07-07：实现完成并归档 `DONE`。真实 CLI run 返回 `DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY`；best recombination candidate=`growth_tilt_lower_turnover_guarded_transfer_v1`，best decision=`OWNER_REVIEW_REQUIRED`，observation preview candidates=0，owner review candidates 包含 `growth_tilt_lower_turnover_guarded_transfer_v1` / `growth_tilt_lower_turnover_guarded_v1`，next route=`TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_Observation_Decision`。candidate auto-accept / research-only observation / paper-shadow / scheduler / event append / outcome binding / production / broker / daily report 全部保持 disabled / false / none。

## 验证结果

- `python -m ruff check .`：PASS。
- `python -m compileall -q src tests`：PASS。
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_component_recombination_candidate_retest.py`：3 passed。
- `python -m ai_trading_system.cli validate-data --as-of 2026-07-05`：`PASS_WITH_WARNINGS`，errors=0，warnings=2，info=12。
- `python -m ai_trading_system.cli research strategies dynamic-strategy-component-recombination-candidate-retest --as-of 2026-07-07`：`DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY`。
- `python -m ai_trading_system.cli docs validate-freshness`：582 docs PASS。
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`：1293 reports PASS。
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`：PASS，active=319，completed=456，failed=0。
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`：PASS，checks=5，failed=0，warnings=0。
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：197 passed，runtime artifact=`outputs/validation_runtime/contract-validation_20260706T173126Z/test_runtime_summary.json`。
- `git diff --check`：PASS（仅 Git CRLF normalization warning，退出码 0）。
