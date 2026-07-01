# Exposure-Cap Mechanics Simulation

TRADING-2323 承接 TRADING-2322，但当前上游仍是 design-only signal validity / aging runtime contract。本报告是 source-blocked simulation readiness package，不读取 runtime records、portfolio exposure history、turnover records 或 post-trigger outcomes，不执行 exposure-cap simulation，不生成 max exposure delta、turnover effect、restore lag 或 false risk-cap cost 结论，不进入 paper-shadow、production 或 broker path。

- status: `EXPOSURE_CAP_MECHANICS_SIMULATION_SOURCE_BLOCKED_NOT_EXECUTED`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `source_blocked_static_simulation_readiness_package`
- data_quality_status: `NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_SIMULATION`
- source_status: `SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_READY_PROMOTION_BLOCKED`
- source_design_only: `True`
- source_runtime_started: `False`
- simulation_objective_count: `4`
- blocked_objective_count: `4`
- input_requirement_count: `20`
- blocker_count: `24`
- executable_simulation_ready: `False`
- simulation_executed: `False`
- simulation_result_generated: `False`
- runtime_records_consumed: `False`
- portfolio_exposure_history_consumed: `False`
- turnover_records_consumed: `False`
- target_weight_generated: `False`
- max_exposure_number_generated: `False`
- broker_order_generated: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Simulation Readiness

|simulation_objective|readiness_status|blocked_reason|
|---|---|---|
|`max_exposure_change_after_risk_cap_trigger`|`SOURCE_BLOCKED_SIMULATION_NOT_EXECUTED`|TRADING-2322 only defines lifecycle schema fields; it does not provide runtime observe records, calibrated multipliers or portfolio exposure state.|
|`cooldown_turnover_effect`|`SOURCE_BLOCKED_SIMULATION_NOT_EXECUTED`|No cooldown runtime state history, turnover history or trade intent log is available from the design-only upstream package.|
|`release_restore_after_risk_cap_clear`|`SOURCE_BLOCKED_SIMULATION_NOT_EXECUTED`|Release / restore rules are design-only and owner-review gated; no runtime clear records or restore decisions exist.|
|`false_risk_cap_cost`|`SOURCE_BLOCKED_SIMULATION_NOT_EXECUTED`|No post-trigger return outcomes, unconstrained exposure path or realized stress labels are available for false-risk-cap cost estimation.|

## Input Requirements

|simulation_objective|requirement_id|requirement_status|
|---|---|---|
|`max_exposure_change_after_risk_cap_trigger`|`runtime_observe_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`max_exposure_change_after_risk_cap_trigger`|`calibrated_cap_multiplier_policy`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`max_exposure_change_after_risk_cap_trigger`|`current_portfolio_exposure_snapshot`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`max_exposure_change_after_risk_cap_trigger`|`exposure_cap_policy_state_history`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`max_exposure_change_after_risk_cap_trigger`|`cached_data_quality_gate`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`cooldown_turnover_effect`|`runtime_observe_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`cooldown_turnover_effect`|`cooldown_state_history`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`cooldown_turnover_effect`|`turnover_history_or_trade_intent_log`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`cooldown_turnover_effect`|`pre_post_cooldown_comparison_window`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`cooldown_turnover_effect`|`cached_data_quality_gate`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`release_restore_after_risk_cap_clear`|`runtime_observe_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`release_restore_after_risk_cap_clear`|`release_restore_decision_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`release_restore_after_risk_cap_clear`|`risk_cap_clear_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`release_restore_after_risk_cap_clear`|`portfolio_exposure_history`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`release_restore_after_risk_cap_clear`|`cached_data_quality_gate`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`false_risk_cap_cost`|`runtime_observe_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`false_risk_cap_cost`|`post_trigger_return_outcomes`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`false_risk_cap_cost`|`benchmark_or_unconstrained_exposure_path`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`false_risk_cap_cost`|`realized_drawdown_or_stress_outcome`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`false_risk_cap_cost`|`cached_data_quality_gate`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|

## Boundary

退出 source-blocked simulation 状态的条件是补齐 runtime observe records、calibrated cap multiplier policy、validated portfolio exposure history、turnover / trade intent history、post-trigger outcomes 和 release / restore decision records，并在读取 cached market/runtime/portfolio data 的 simulation workflow 中执行 `aits validate-data` 或同源 data-quality gate。当前不得把本报告解读为 exposure-cap mechanics simulation result。
