# TRADING-2392 Dynamic Strategy Component Attribution And Gate Evidence Plan

最后更新：2026-07-07

## 状态

- 当前状态：`DONE`
- 创建日期：2026-07-07
- 任务登记：`TRADING-2392_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN`
- 目标状态：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`
- 上游 owner decision：`DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`
- 下一路由：`TRADING-2393_Dynamic_Strategy_Component_Attribution_Targeted_Ablation_Retest`

## 背景

TRADING-2391 已记录 calibrated gate candidate owner review decision：current best candidate 保留 `OWNER_REVIEW_REQUIRED`，但不批准 observation、paper-shadow 或执行链路。当前问题从“候选是否可直接观察”转为“整体未达标候选中哪些组件值得复用、如何补足 gate evidence”。

本任务是 plan-only / evidence-design，不运行 ablation retest，不生成新 signal，不修改真实 gate，也不批准任何 observation。

## 范围

允许动作：

- 读取 TRADING-2365 / 2366 / 2386 / 2390 / 2391 prior validated research artifacts。
- 提取 component-value candidates。
- 生成 component attribution plan。
- 生成 component value matrix。
- 生成 gate evidence plan。
- 生成 targeted ablation retest plan 和 TRADING-2393 route。
- 更新 registry、artifact catalog、system flow、task register 和完成归档文档。

禁止动作：

- 运行新 backtest、ablation retest、signal、features、scoring 或 daily report。
- 批准 candidate auto-accept 或 research-only observation。
- 启用 scheduler、创建 scheduled task 或接入 daily-run。
- append historical event log、bind outcome 或 mutate outcome store。
- 启用 paper-shadow、创建 paper trade 或 shadow position。
- 启用 production、调用 broker API、发送 order 或生成 target weight / rebalance instruction。
- 修改真实 gate、阈值、score band、promotion gate、backtest acceptance rule、position constraint 或数据质量门。

## 数据质量门禁边界

本任务默认不运行 `aits validate-data --as-of 2026-07-05`，因为实现只读取 prior validated TRADING-2365 / 2366 / 2386 / 2390 / 2391 artifacts，不读取 fresh cached market data、不重新 backtest、不生成 technical features、scoring、daily report 或交易建议。

如果实现中改为读取 cached market/macro data、重新 backtest、生成 signal/scoring 或 daily report，则必须先运行 `aits validate-data` 或同源质量门，并在输出中披露质量状态。

## 拆解

1. 新增 builder `src/ai_trading_system/dynamic_strategy_component_attribution_gate_evidence_plan.py`。
2. 新增 CLI `aits research strategies dynamic-strategy-component-attribution-gate-evidence-plan`。
3. fail-closed 校验 TRADING-2365 / 2366 / 2386 / 2390 / 2391 source status、owner decision、component-value candidates、2391 non-approval 和 safety fields。
4. 输出 `component_attribution_plan.json`、`component_value_matrix.json`、`gate_evidence_plan.json` 和 `targeted_ablation_retest_plan.json`。
5. 生成 research docs：main report、component value matrix、gate evidence plan 和 TRADING-2393 route。
6. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、任务登记和完成归档。
7. 新增 focused builder / CLI / registry-doc tests。
8. 运行 focused validation、真实 CLI、文档/registry/task-register/contract checks。

## 验收标准

- CLI 真实 run 返回 `DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`。
- 输出明确包含：
  - `component_attribution_plan_ready=true`
  - `component_value_matrix_ready=true`
  - `gate_evidence_plan_ready=true`
  - `targeted_ablation_retest_plan_ready=true`
  - `component_value_candidates` 包含 `dynamic_turnover_budgeted_growth_tilt_v1` 和 `dynamic_valid_until_expiry_strict_v1`
  - `components_to_attribute` 包含 `turnover_budgeting`、`valid_until_strictness`、`growth_tilt_engine`、`lower_turnover_guardrail`、`guarded_turnover_transfer`
  - `recommended_next_research_task=TRADING-2393_Dynamic_Strategy_Component_Attribution_Targeted_Ablation_Retest`
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
- Focused tests、Ruff、compileall、真实 CLI、docs freshness、report contract、task-register consistency、contract-validation 和 `git diff --check` 通过。

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-07：实现完成并归档 `DONE`。真实 CLI run 返回 `DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`；component attribution plan、component value matrix、gate evidence plan 和 targeted ablation retest plan 均 ready；下一路由限定为 `TRADING-2393_Dynamic_Strategy_Component_Attribution_Targeted_Ablation_Retest`。

## 验证计划

- `python -m ruff check .`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/research_strategies/test_dynamic_strategy_component_attribution_gate_evidence_plan.py`
- 真实 CLI run：`aits research strategies dynamic-strategy-component-attribution-gate-evidence-plan --as-of 2026-07-07`
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-07`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 验证结果

- 初始实现验证通过 focused Ruff、`compileall -q src tests/research_strategies/test_dynamic_strategy_component_attribution_gate_evidence_plan.py`、focused parallel pytest 3 passed、真实 CLI run、docs freshness 578 docs PASS、documentation contract 1289 reports PASS、task-register consistency run active=320 / completed=451 / failed=0。
- 归档后验证通过 full Ruff、`compileall -q src tests`、focused parallel pytest 3 passed、真实 CLI run、docs freshness 578 docs PASS、documentation contract 1289 reports PASS、task-register consistency run active=319 / completed=452 / failed=0、task-register consistency validate checks=5 / failed=0 / warnings=0、contract-validation 197 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260706T154253Z/test_runtime_summary.json`）和 `git diff --check`（仅 CRLF normalization warning）。
