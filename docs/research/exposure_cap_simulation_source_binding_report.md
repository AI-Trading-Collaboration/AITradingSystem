# Exposure-Cap Simulation Source Binding

TRADING-2324 将 TRADING-2323 source-blocked simulation readiness 绑定到 risk-cap trigger series、cached market price history、portfolio baseline 和 policy-governed turnover / cooldown / exposure-cap assumptions。本报告只是 source-bound dry-run readiness 与 proxy diagnostics，不产生 target weight、rebalance instruction、paper-shadow、production 或 broker action。

- status: `EXPOSURE_CAP_SIMULATION_SOURCE_BOUND_DRY_RUN_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2023-01-06..2026-06-18`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_quality_gate_executed: `True`
- dry_run_readiness_status: `SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`
- portfolio_source_mode: `synthetic_observe_only`
- dry_run_record_count: `2595`
- cap_binding_days: `382`
- next_task: `TRADING-2325_Portfolio_Baseline_Source_Decision`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Proxy Comparison

- baseline_return_proxy: `3.987211`
- capped_return_proxy: `3.823477`
- return_proxy_delta: `-0.163734`
- turnover_proxy_total: `41.5`
- interpretation_boundary: `proxy_diagnostics_only_synthetic_observe_baseline`
