# Dynamic Target Exposure-Cap Dry-Run Report

TRADING-2332 使用 TRADING-2331 允许的 timestamp-remediated dynamic target baseline wrapper 执行 source-bound exposure-cap dry-run。wrapper 仍为 `PIT_APPROXIMATION_READY`，并使用 `NEXT_SESSION_DECISION_POLICY`；本报告只提供 research-only proxy diagnostics。

- status: `SOURCE_BOUND_DYNAMIC_TARGET_EXPOSURE_CAP_DRY_RUN_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- target_assets: `QQQ, SGOV, TQQQ`
- data_quality_gate_executed: `True`
- data_quality_status: `PASS_WITH_WARNINGS`
- warning_count: `2683`
- error_count: `0`
- simulation_executed: `True`
- cap_binding_days: `378`
- cap_binding_rate: `0.455422`
- average_exposure_reduction: `0.075904`
- exposure_reduction_label: `INCREMENTAL_EXPOSURE_REDUCTION_MATERIAL`
- return_proxy_delta: `-0.187258`
- drawdown_proxy_delta: `0.045294`
- return_drawdown_tradeoff_label: `DRAWDOWN_IMPROVED_RETURN_COSTLY`
- overlap_label: `RISK_CAP_BINDING_WHEN_DYNAMIC_MISSES_RISK`
- next_task: `TRADING-2333_Dynamic_Exposure_Cap_vs_No_Cap_Diagnostics_Review`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## 解释边界

本报告不得解释为真实账户表现、真实仓位建议、paper-shadow signal、production strategy 或 broker action；dynamic no-cap / capped 差异只是 PIT caveat wrapper 下的 proxy diagnostics。
