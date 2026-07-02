# Exposure-Cap vs No-Cap Diagnostics Review

TRADING-2327 只读取 TRADING-2326 已验证的 static ETF baseline dry-run artifacts，不重新执行 simulation，也不重新消费 cached market data。所有结论都是 research proxy diagnostics，不得解释为真实账户表现、真实仓位建议、paper-shadow signal、production strategy 或 broker action。

- status: `EXPOSURE_CAP_DIAGNOSTICS_REVIEW_READY_PROMOTION_BLOCKED`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_validation_policy: `NOT_APPLICABLE_PRIOR_VALIDATED_DRY_RUN_ARTIFACTS_ONLY`
- simulation_start: `2023-01-06`
- simulation_end: `2026-06-18`
- cap_binding_days: `382`
- cap_binding_rate: `0.441618`
- cap_binding_frequency_label: `EXCESSIVE_BINDING_FREQUENCY`
- exposure_reduction_label: `EXPOSURE_REDUCTION_MODEST`
- return_proxy_delta: `-0.174103`
- drawdown_proxy_delta: `0.06548`
- return_drawdown_tradeoff_label: `DRAWDOWN_IMPROVED_RETURN_COSTLY`
- turnover_cooldown_label: `TURNOVER_HIGH_AND_COOLDOWN_COSTLY`
- false_cost_label: `FALSE_COST_BLOCKING`
- missed_upside_label: `MISSED_UPSIDE_BLOCKING`
- downside_protection_label: `DOWNSIDE_PROTECTION_POSITIVE_PROXY`
- overall_recommendation: `MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION`
- next_task: `TRADING-2328_Dynamic_Target_Baseline_Preparation`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## 解释边界

本报告不能用于真实减仓、真实调仓、paper-shadow、production 或 broker action。若后续进入 dynamic baseline 或 policy refinement，需要单独的 source binding、data-quality gate、replayability audit 和 owner review。
