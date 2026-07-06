# TRADING-2395 Dynamic Strategy Component Recombination Candidate Plan

最后更新：2026-07-07

## 状态

- 状态：`DONE`
- 任务登记：`TRADING-2395_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN`
- 优先级：P0
- 下一路由：`TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest`
- 任务边界：plan-only，不运行新 backtest，不生成 new signal，不批准 observation / paper-shadow / execution。

## 背景

TRADING-2394 已记录 owner decision=`APPROVE_COMPONENT_RECOMBINATION_PLAN_WITH_NO_OBSERVATION_APPROVAL`，并确认：

- `growth_tilt_engine` 是可复用 return engine。
- `lower_turnover_guardrail` 只能作为 execution / risk guardrail。
- `guarded_turnover_transfer` 仍需 owner review。
- `valid_until_window` 和 `no_stale_signal_carry_forward` 必须保留。

TRADING-2395 把这些组件结论转成 2396 可以实际 retest 的 recombination candidate plan。

## 非目标与安全边界

本任务不得：

- run new backtest；
- generate new signal / scoring；
- approve research-only observation；
- enable scheduler / event append / outcome binding；
- enable paper-shadow / paper trade / shadow position；
- enable production / broker / order；
- generate daily report；
- use monthly rebalance as primary decision cadence。

允许读取 prior validated TRADING-2391 / 2392 / 2393 / 2394 artifacts，生成 candidate plan、candidate definitions、forbidden recombination paths、2396 retest plan、acceptance criteria 和 route。

## 实施步骤

1. 新增 `src/ai_trading_system/dynamic_strategy_component_recombination_candidate_plan.py`。
2. 新增 CLI：`aits research strategies dynamic-strategy-component-recombination-candidate-plan`。
3. Fail-closed 校验 2394 owner review decision、2393 component decisions、2392 attribution plan 和 source safety fields。
4. 输出 `recombination_candidate_plan.json`、`recombination_candidate_definitions.json`、`retest_plan_2396.json`、`recombination_acceptance_criteria.json`。
5. 生成 research docs：主报告、candidate definitions、retest plan、2396 route。
6. 更新 report registry、artifact catalog、system flow、task register 和 completed archive。
7. 新增 focused tests。

## 验收标准

真实 CLI run 必须返回：

- `status=DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY`
- `owner_decision_from_2394=APPROVE_COMPONENT_RECOMBINATION_PLAN_WITH_NO_OBSERVATION_APPROVAL`
- `recombination_candidate_plan_ready=true`
- `recombination_candidate_definitions_ready=true`
- `retest_plan_2396_ready=true`
- `acceptance_criteria_ready=true`
- `return_engine_component=growth_tilt_engine`
- `guardrail_components` 包含 `lower_turnover_guardrail`、`valid_until_window`、`no_stale_signal_carry_forward`
- `owner_review_components=["guarded_turnover_transfer"]`
- `planned_recombination_candidates` 至少包含附件列出的 6 个 candidates
- `recommended_next_research_task=TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest`
- candidate auto-accept、research-only observation、paper-shadow、scheduler、event append、outcome binding、production、broker、daily report 全部为 false / none

## 数据质量门禁

如果实现只读取 prior validated artifacts，不读取 fresh cached market data、不运行新 backtest、不生成 technical features / scoring / daily report 或交易建议，则不运行 `aits validate-data --as-of 2026-07-05`。完成说明必须明确该原因。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_component_recombination_candidate_plan.py`
- 真实 CLI run：`aits research strategies dynamic-strategy-component-recombination-candidate-plan --as-of 2026-07-07`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。本任务只设计 recombination candidates 和 2396 retest plan，不批准 observation、paper-shadow、scheduler、event append、outcome binding、production、broker 或 daily report。
- 2026-07-07：实现完成并归档 `DONE`。真实 CLI run 返回 `DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY`，owner decision from 2394=`APPROVE_COMPONENT_RECOMBINATION_PLAN_WITH_NO_OBSERVATION_APPROVAL`，return engine=`growth_tilt_engine`，guardrail components 包含 `lower_turnover_guardrail` / `valid_until_window` / `no_stale_signal_carry_forward`，owner-review component=`guarded_turnover_transfer`，planned recombination candidates=6，next route=`TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest`。本任务只读取 prior validated TRADING-2390 / 2391 / 2392 / 2393 / 2394 artifacts，因此未运行 `aits validate-data --as-of 2026-07-05`；candidate auto-accept / research-only observation / paper-shadow / scheduler / event append / outcome binding / production / broker / daily report 全部保持 disabled / false / none。

## 验证结果

- 初始实现与归档后验证通过 full Ruff、`compileall -q src tests`、focused parallel pytest 3 passed、真实 2395 CLI run、docs freshness 581 docs PASS、documentation contract 1292 reports PASS、task-register consistency run active=319 / completed=455 / failed=0、task-register consistency validate checks=5 / failed=0 / warnings=0、active register DONE-row check clean、contract-validation 197 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260706T171325Z/test_runtime_summary.json`），以及 `git diff --check` PASS（仅 Git CRLF normalization warning，退出码 0）。
