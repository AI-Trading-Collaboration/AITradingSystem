# TRADING-2393 Dynamic Strategy Component Attribution Targeted Ablation Retest

最后更新：2026-07-07

## 状态

- 当前状态：`DONE`
- 创建日期：2026-07-07
- 任务登记：`TRADING-2393_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST`
- 目标状态：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY`
- 上游计划：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`
- 下一路由：`TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_Recombination_Decision`

## 背景

TRADING-2392 已完成 component attribution and gate evidence plan，并将下一步限定为 targeted ablation retest。2393 需要实际运行组件消融回测，验证 `turnover_budgeting`、`valid_until_strictness`、`growth_tilt_engine`、`lower_turnover_guardrail`、`guarded_turnover_transfer` 是否有可复用价值。

## 范围

允许动作：

- 读取 TRADING-2365 / 2366 / 2386 / 2390 / 2391 / 2392 prior research artifacts。
- 执行 `aits validate-data --as-of 2026-07-05` 或同源数据质量门。
- 运行 targeted ablation backtest。
- 计算 component attribution、cost-adjusted、turnover、stale signal、time/regime slice metrics。
- 生成 reusable component decision 和 TRADING-2394 route。

禁止动作：

- 批准 candidate auto-accept 或 research-only observation。
- 启用 scheduler、创建 scheduled task 或接入 daily-run。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 启用 production、调用 broker API、发送 order 或生成 daily report。
- 使用 monthly rebalance 作为 primary decision。

## 数据质量门禁边界

本任务会读取 cached market data 并运行 targeted ablation / backtest，因此必须先运行 `aits validate-data --as-of 2026-07-05` 或同源质量门；输出必须披露 `data_quality_gate_executed=true`、status、report path 和 pass/fail。

## 拆解

1. 新增 builder `src/ai_trading_system/dynamic_strategy_component_attribution_targeted_ablation_retest.py`。
2. 新增 CLI `aits research strategies dynamic-strategy-component-attribution-targeted-ablation-retest`。
3. fail-closed 校验 2365 / 2366 / 2386 / 2390 / 2391 / 2392 source status、2392 components、2392 ablation plan、2391 non-approval 和 safety fields。
4. 构造并运行六个 ablation candidates：`growth_tilt_only_reference`、`growth_tilt_plus_turnover_budget`、`growth_tilt_plus_valid_until_strict`、`growth_tilt_plus_turnover_budget_and_valid_until`、`lower_turnover_without_cooldown`、`lower_turnover_plus_growth_tilt_component`。
5. 生成 `ablation_retest_result.json`、`component_attribution_matrix.json`、`reusable_component_decision.json` 和 `decision_update.json`。
6. 生成 research docs：主报告、ablation result、reusable component decision 和 TRADING-2394 route。
7. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、任务登记和完成归档。
8. 新增 focused builder / CLI / registry-doc tests。
9. 运行 focused validation、真实 CLI、文档/registry/task-register/contract checks。

## 验收标准

- CLI 真实 run 返回 `DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY`。
- 输出明确包含：
  - `ablation_retest_ready=true`
  - `component_attribution_matrix_ready=true`
  - `reusable_component_decision_ready=true`
  - `components_tested` 覆盖 `turnover_budgeting`、`valid_until_strictness`、`growth_tilt_engine`、`lower_turnover_guardrail`、`guarded_turnover_transfer`
  - `ablation_candidates_tested` 覆盖六个指定 ablation candidates
  - `component_decisions_ready=true`
  - `best_reusable_component`
  - `recommended_next_research_task=TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_Recombination_Decision`
  - `candidate_auto_accept_approved=false`
  - `research_only_observation_approved=false`
  - `paper_shadow_enabled=false`
  - `event_append_enabled=false`
  - `outcome_binding_enabled=false`
  - `scheduler_enabled=false`
  - `production_enabled=false`
  - `broker_action_enabled=false`
  - `daily_report_generated=false`
- Registry、artifact catalog、system flow、task register 和 completed archive 一致。
- `aits validate-data --as-of 2026-07-05`、focused tests、Ruff、compileall、真实 CLI、docs freshness、report contract、task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-07：实现完成并归档 `DONE`。真实 `aits validate-data --as-of 2026-07-05` 返回 `PASS_WITH_WARNINGS` / errors=0；真实 CLI run 返回 `DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY`，best reusable component=`growth_tilt_engine`，next route=`TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_Recombination_Decision`，candidate auto-accept / research-only observation / paper-shadow / scheduler / event append / outcome binding / production / broker / daily report 全部保持 disabled / false / none。

## 验证计划

- `python -m ai_trading_system.cli validate-data --as-of 2026-07-05`
- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_component_attribution_targeted_ablation_retest.py`
- 真实 CLI run：`aits research strategies dynamic-strategy-component-attribution-targeted-ablation-retest --as-of 2026-07-05`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 验证结果

- 初始实现验证通过 focused Ruff、`compileall -q src/ai_trading_system/dynamic_strategy_component_attribution_targeted_ablation_retest.py src/ai_trading_system/cli_commands/research_execution_semantics.py tests/research_strategies/test_dynamic_strategy_component_attribution_targeted_ablation_retest.py`、focused parallel pytest 3 passed、`aits validate-data --as-of 2026-07-05` PASS_WITH_WARNINGS / errors=0 和真实 2393 CLI run。
- 归档后验证通过 full Ruff、`compileall -q src tests`、focused parallel pytest 3 passed、docs freshness 579 docs PASS、documentation contract 1290 reports PASS、task-register consistency run active=319 / completed=453 / failed=0、task-register consistency validate checks=5 / failed=0 / warnings=0、contract-validation 197 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260706T161550Z/test_runtime_summary.json`）和 `git diff --check`（仅 CRLF normalization warning）。本任务无 observation、paper-shadow、scheduler、event append、outcome binding、production、broker 或 daily report 副作用。
