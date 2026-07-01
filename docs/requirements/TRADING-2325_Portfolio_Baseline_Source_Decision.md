# TRADING-2325 Portfolio Baseline Source Decision

最后更新：2026-07-01

## Status

- task_id: `TRADING-2325_PORTFOLIO_BASELINE_SOURCE_DECISION`
- status: `VALIDATING`
- priority: `P0`
- owner: system implementation + project owner review
- last_update: 2026-07-01

## Background

TRADING-2324 已经把 TRADING-2323 exposure-cap mechanics readiness 绑定到
risk-cap trigger series、validated cached market data、turnover / rebalance
assumptions、cooldown / cap policy 和 synthetic observe-only portfolio baseline。
真实 run 达到 `SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`，但该结果仍然
只能解释为 source-bound proxy diagnostics，不能解释为真实 portfolio simulation。

TRADING-2325 的目标是决定 TRADING-2326 exposure-cap dry-run simulation 应该使用哪一种
portfolio / exposure baseline source。

## Scope

本任务新增 `aits research trends portfolio-baseline-source-decision`。命令只读取：

- TRADING-2324 exposure-cap source binding outputs；
- TRADING-2323 exposure-cap mechanics readiness package；
- static ETF allocation config；
- paper portfolio config；
- 可选 actual holdings source metadata。

本任务不读取 cached market data，不执行新的 exposure-cap simulation，不生成 target weight，
不生成 rebalance instruction，不启动 paper-shadow、production 或 broker action。

## Baseline Candidates

必须审计并比较：

- `synthetic_observe_only_baseline`
- `static_etf_allocation_baseline`
- `dynamic_strategy_target_exposure_baseline`
- `paper_portfolio_advisory_baseline`
- `actual_holdings_derived_baseline`

默认短期路线应优先选择 `static_etf_allocation_baseline`，保留
`synthetic_observe_only_baseline` 作为 fallback。`dynamic_strategy_target_exposure_baseline`
作为中期路线，等待 PIT target exposure artifact。`actual_holdings_derived_baseline`
当前只允许 owner-only manual reference，不作为研究层 baseline source。

## Implementation Steps

1. 新增 loader，读取并 fail-closed 校验 TRADING-2324 / TRADING-2323 upstream safety fields。
2. 生成 baseline candidate matrix，覆盖可用性、coverage、PIT、复现性、维护成本、解释价值和隐私风险。
3. 生成 feasibility matrix、PIT / reproducibility audit、risk matrix 和 field requirement matrix。
4. 生成 recommended simulation baseline spec 和 TRADING-2326 route recommendation。
5. 写出 runtime JSON artifacts 和 research Markdown reports。
6. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 和 `docs/task_register.md`。
7. 增加 focused parallel pytest，并运行项目要求的文档、contract、task-register 和 diff validation。

## Acceptance Criteria

- CLI `aits research trends portfolio-baseline-source-decision` 可运行。
- 输出 `portfolio_baseline_source_decision_summary.json`。
- 输出 `portfolio_baseline_candidate_matrix.json`。
- 输出 `portfolio_baseline_source_feasibility_matrix.json`。
- 输出 `portfolio_baseline_pit_reproducibility_audit.json`。
- 输出 `portfolio_baseline_risk_matrix.json`。
- 输出 `portfolio_baseline_field_requirement_matrix.json`。
- 输出 `recommended_exposure_cap_simulation_baseline.json`。
- 输出 `exposure_cap_2326_task_route.json`。
- 输出 `portfolio_baseline_source_safety_boundary.json`。
- research docs 说明 TRADING-2324 synthetic baseline 的解释边界和 TRADING-2326 route。
- `simulation_executed=false`。
- `promotion_allowed=false`。
- `paper_shadow_allowed=false`。
- `production_allowed=false`。
- `broker_action=none`。
- 最终报告说明 `aits validate-data` 不适用，原因是本任务只读取 static config / prior research outputs。

## Validation Plan

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for all six TRADING-2325 test files
- full parallel pytest through project-standard parallel validation
- docs freshness
- documentation contract
- contract-validation tier
- task-register consistency run / validate
- `git diff --check`

## Progress Notes

- 2026-07-01: 根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 存在两个既有无关 research docs 改动，本任务必须 selective staging，不得混入本次 commit。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 run status=`PORTFOLIO_BASELINE_SOURCE_DECISION_READY_PROMOTION_BLOCKED`，selected_baseline_for_2326=`static_etf_allocation_baseline`，fallback_baseline=`synthetic_observe_only_baseline`，next_task=`TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Static_ETF_Baseline`，data_quality_status=`NOT_APPLICABLE_SOURCE_DECISION_ONLY`，`aits_validate_data_executed=false`，原因是本任务只读取 static config / prior research outputs。验证通过 Ruff、compileall、focused parallel pytest 28 passed、full parallel pytest 3953 passed、docs freshness、documentation contract 1223 reports、contract-validation 193 passed、task-register consistency run/validate 和 `git diff --check`；所有 outputs 固定 promotion/paper-shadow/production/broker false/none。
