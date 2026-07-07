# TRADING-2407 Valid-Until Window Semantics And Stale Signal Remediation Plan

最后更新：2026-07-07

## 状态

- status：`DONE`
- priority：`P0`
- owner：系统实现 + 项目 owner 后续复核
- created：2026-07-07
- source tasks：`TRADING-2403` / `TRADING-2405` / `TRADING-2406`
- input under review：`valid_until_window`
- current severity：`BLOCKING`
- current PIT status：`UNKNOWN_OR_APPROXIMATE_PIT`

## 背景

TRADING-2405 reusable PIT gate 把 `growth_tilt_engine` 与 `valid_until_window` 标为当前 dynamic strategy candidate search 的两个 blocking gaps。TRADING-2406 已将 `growth_tilt_engine` 的 source feature、PIT risk、signal construction 和 remediation route 固化为 plan-only evidence，并明确把第二个 blocking gap 路由到本任务。

本任务只把 `valid_until_window` 转成可实施的 remediation plan：审计 valid-from / valid-until / expiry / stale carry-forward / signal-to-execution lag 语义，定义 signal validity contract、growth tilt alignment review、severity downgrade 条件和 replay validation plan。2407 不实现交易逻辑、不清除 blocker、不降级 severity，也不恢复候选搜索。

## 输入

- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/implementation_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_input_registry_snapshot.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_coverage_matrix.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_gate_result.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_blocker_summary.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_matrix_reusable_implementation/pit_remediation_routes.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/remediation_plan_result.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/source_feature_inventory.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/pit_risk_audit.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/signal_construction_gap_analysis.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/severity_downgrade_conditions.json`
- `outputs/research_strategies/dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan/validation_plan.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/pit_coverage_matrix.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/signal_construction_review.json`
- `outputs/research_strategies/dynamic_strategy_pit_coverage_signal_construction_review/remediation_matrix.json`
- `config/research/dynamic_strategy_pit_input_registry.yaml`
- `config/research/strategy_execution_policy_registry.yaml`
- `config/research/signal_validity_taxonomy.yaml`

## 产物

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

## 阶段拆解

1. 更新 task register 与本需求文档，明确 2407 只做 plan-only remediation evidence。
2. 新增 `dynamic_strategy_valid_until_window_stale_signal_remediation_plan` builder，fail-closed 读取 2403 / 2405 / 2406 artifacts 与 governed configs。
3. 新增 CLI `aits research strategies dynamic-strategy-valid-until-window-stale-signal-remediation-plan`。
4. 生成 valid-until semantics review、stale signal risk audit、signal validity contract plan、growth tilt alignment review、remediation plan、severity downgrade conditions、validation plan 和 2408 route。
5. 更新 report registry、artifact catalog、system flow、task register / completed archive 与 completed task note。
6. 添加 focused pytest，运行真实 CLI 和文档/合同门禁。

## 验收标准

- CLI 返回 status=`DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN_READY`。
- 输出必须包含 `valid_until_window_blocking_gap_resolved=false`、`valid_until_window_remediation_plan_ready=true`、`valid_until_window_validation_plan_ready=true`、`valid_until_window_severity_downgraded=false`。
- `candidate_search_allowed=false`、`candidate_search_resumed=false`、`research_only_observation_allowed=false`、`research_only_observation_approved=false`、`paper_shadow_allowed=false`、`paper_shadow_enabled=false`、`event_append_enabled=false`、`outcome_binding_enabled=false`、`scheduler_enabled=false`、`production_enabled=false`、`broker_action_enabled=false`、`daily_report_generated=false`。
- `valid_until_semantics_review_ready=true`、`stale_signal_risk_audit_ready=true`、`signal_validity_contract_plan_ready=true`、`growth_tilt_alignment_review_ready=true`、`remediation_plan_ready=true`、`severity_downgrade_conditions_ready=true`、`validation_plan_ready=true`。
- `current_blocker.input_id=valid_until_window`、`input_type=EXECUTION_SEMANTIC`、`severity=BLOCKING`、`candidate_search_blocker=true`。
- 下一任务固定为 `TRADING-2408_Dynamic_Strategy_Blocking_Gap_Remediation_Implementation_Plan`。
- 不运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/event data；若跳过，输出和验收记录必须说明原因。

## 安全边界

2407 禁止执行：

- clear / downgrade `valid_until_window` blocker
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
- 2026-07-07：实现完成并归档 `DONE`；真实 CLI run ready，未清除或降级 `valid_until_window` blocker，下一步限定为 `TRADING-2408_Dynamic_Strategy_Blocking_Gap_Remediation_Implementation_Plan`。
