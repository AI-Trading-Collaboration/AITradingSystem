# TRADING-2408 Dynamic Strategy Blocking Gap Remediation Implementation Plan

最后更新：2026-07-07

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-07
- source tasks：`TRADING-2405` / `TRADING-2406` / `TRADING-2407`
- blocking gaps：`growth_tilt_engine` / `valid_until_window`

## 背景

TRADING-2405 reusable PIT gate 识别 `growth_tilt_engine` 与 `valid_until_window` 为当前 dynamic strategy candidate search 的两个 blocking gaps。TRADING-2406 已为 `growth_tilt_engine` 生成 source feature / PIT / signal construction remediation plan。TRADING-2407 已为 `valid_until_window` 生成 valid-until / stale-signal remediation plan。

TRADING-2408 的目标是把这两个 plan 合并为统一 implementation plan，明确后续 contract/schema、mapping、replay validation、owner downgrade review 与 candidate gate 重新评估的执行顺序。2408 不直接实现完整 schema/replay，不清除 blocker，不降级 severity，也不恢复 candidate search。

## 输入

- TRADING-2405：
  - `implementation_result.json`
  - `pit_input_registry_snapshot.json`
  - `pit_coverage_matrix.json`
  - `pit_gate_result.json`
  - `pit_blocker_summary.json`
  - `pit_remediation_routes.json`
- TRADING-2406：
  - `remediation_plan_result.json`
  - `source_feature_inventory.json`
  - `pit_risk_audit.json`
  - `signal_construction_gap_analysis.json`
  - `severity_downgrade_conditions.json`
  - `validation_plan.json`
- TRADING-2407：
  - `remediation_plan_result.json`
  - `valid_until_semantics_review.json`
  - `stale_signal_risk_audit.json`
  - `signal_validity_contract_plan.json`
  - `severity_downgrade_conditions.json`
  - `validation_plan.json`
- Config：
  - `config/research/dynamic_strategy_pit_input_registry.yaml`

## 产物

- `outputs/research_strategies/dynamic_strategy_blocking_gap_remediation_implementation_plan/implementation_plan_result.json`
- `outputs/research_strategies/dynamic_strategy_blocking_gap_remediation_implementation_plan/unified_remediation_architecture.json`
- `outputs/research_strategies/dynamic_strategy_blocking_gap_remediation_implementation_plan/contract_schema_plan.json`
- `outputs/research_strategies/dynamic_strategy_blocking_gap_remediation_implementation_plan/implementation_sequence.json`
- `outputs/research_strategies/dynamic_strategy_blocking_gap_remediation_implementation_plan/blocker_downgrade_workflow.json`
- `outputs/research_strategies/dynamic_strategy_blocking_gap_remediation_implementation_plan/candidate_search_gate_policy.json`
- `docs/research/dynamic_strategy_blocking_gap_remediation_implementation_plan.md`
- `docs/research/dynamic_strategy_signal_as_of_and_validity_contract_schema_plan.md`
- `docs/research/dynamic_strategy_blocker_downgrade_workflow.md`
- `docs/research/dynamic_strategy_blocking_gap_implementation_sequence.md`
- `docs/research/dynamic_strategy_2409_route.md`

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2408 只做 implementation plan。
2. 新增 `dynamic_strategy_blocking_gap_remediation_implementation_plan` builder，fail-closed 读取 2405 / 2406 / 2407 artifacts 与 PIT registry。
3. 新增 CLI `aits research strategies dynamic-strategy-blocking-gap-remediation-implementation-plan`。
4. 生成 unified remediation architecture、contract schema plan、implementation sequence、blocker downgrade workflow、candidate search gate policy 和 2409 route。
5. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
6. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_READY`。
- `blocking_gaps` 包含 `growth_tilt_engine` 和 `valid_until_window`。
- `unified_remediation_architecture_ready=true`、`contract_schema_plan_ready=true`、`implementation_sequence_ready=true`、`blocker_downgrade_workflow_ready=true`、`candidate_search_gate_policy_ready=true`。
- `growth_tilt_engine_blocking_gap_resolved=false`、`valid_until_window_blocking_gap_resolved=false`、`any_blocker_severity_downgraded=false`。
- `automatic_downgrade_allowed=false`、`owner_review_required_for_any_downgrade=true`。
- `candidate_search_allowed=false`、`candidate_search_resumed=false`、`research_only_observation_allowed=false`、`research_only_observation_approved=false`、`paper_shadow_allowed=false`、`paper_shadow_enabled=false`、`event_append_enabled=false`、`outcome_binding_enabled=false`、`scheduler_enabled=false`、`production_enabled=false`、`broker_action_enabled=false`、`daily_report_generated=false`。
- 下一任务固定为 `TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2408 禁止执行：

- clear / downgrade `growth_tilt_engine` 或 `valid_until_window`
- mark TRUE_PIT
- resume candidate search
- approve research-only observation
- enable paper-shadow, paper trade, shadow position
- append event or bind outcome
- create scheduler / daily report
- run new strategy backtest
- generate new trading signal or scoring output
- enable production or broker/order path

## 进展记录

- 2026-07-07：根据 owner 附件新增并进入 `IN_PROGRESS`。
- 2026-07-07：实现完成并归档 `DONE`。新增 builder / CLI / JSON artifacts / research docs / registry / catalog / system flow / focused tests；真实 CLI run 返回 status=`DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_READY`，`route_to_next_task=TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema`，两个 blocker 均保持 unresolved / undowngraded，所有 safety fields 保持 false / none。
- 2026-07-07：未运行 `aits validate-data --as-of 2026-07-05`，因为本任务仅读取 prior validated TRADING-2405 / 2406 / 2407 artifacts 与 governed PIT input registry，不读取 fresh cached market data、不运行新 backtest、不生成 technical features、scoring、daily report 或交易建议。
