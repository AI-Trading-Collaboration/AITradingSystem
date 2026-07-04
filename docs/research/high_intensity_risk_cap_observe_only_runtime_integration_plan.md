# High-Intensity Risk-Cap Observe-Only Runtime Integration Plan

- status: `OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`
- selected_rule: `COMPOSITE_HIGH_INTENSITY_RULE`
- readiness: `READY_FOR_2343_WITH_CAVEATS`
- next_task: `TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run`
- source_validate_data: `2026-06-29` / `PASS_WITH_WARNINGS` / error_count=`0`

本报告只生成 observe-only runtime integration plan；不启动 runtime scheduler，不生成新 event，不 append runtime log，不绑定 outcome，不输出 target weight / rebalance / broker action。

## Runtime Scope

- runtime_mode: `observe_only`
- runtime_scheduler_enabled: `False`
- event_append_allowed_for_next_task: `True`
- partial_coverage_caveat_required: `True`
- monthly_concentration_monitoring_required: `True`

## Route

- route_caveats: `['PIT_APPROXIMATION_CAVEAT', 'MONTHLY_CONCENTRATION_MONITORING_REQUIRED', 'PARTIAL_COVERAGE_CAVEAT', 'OBSERVE_ONLY_NO_PAPER_SHADOW']`
- promotion / paper-shadow / production / broker action: blocked
