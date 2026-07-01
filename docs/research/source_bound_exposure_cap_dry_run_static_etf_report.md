# Static ETF Baseline Source-Bound Exposure-Cap Dry-Run

TRADING-2326 使用 TRADING-2325 选定的 `static_etf_allocation_baseline` 执行 source-bound exposure-cap dry-run simulation。这里的 cap 解释为组合层面的总风险资产敞口上限，再按 QQQ / SPY / SMH baseline 权重等比例缩放风险资产；`CASH` 保持 static baseline 权重。static ETF baseline 不是实际账户，所有 return / drawdown / turnover 数值都是 proxy diagnostics。

- status: `SOURCE_BOUND_STATIC_ETF_EXPOSURE_CAP_DRY_RUN_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- selected_baseline: `static_etf_allocation_baseline`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_quality_gate_executed: `True`
- simulation_start: `2023-01-06`
- simulation_end: `2026-06-18`
- cap_binding_days: `382`
- cap_binding_rate: `0.441618`
- average_exposure_reduction: `0.038642`
- return_proxy_delta: `-0.174103`
- drawdown_proxy_delta: `0.06548`
- turnover_impact_label: `TURNOVER_IMPACT_INCONCLUSIVE`
- cooldown_impact_label: `COOLDOWN_COSTLY_PROXY`
- false_risk_cap_cost_proxy: `0.850693`
- downside_protection_proxy: `0.67659`
- next_task: `TRADING-2327_Exposure_Cap_vs_No_Cap_Diagnostics_Review`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## 解释边界

本报告不得解释为真实账户表现、真实仓位建议、paper-shadow signal、production strategy 或 broker action；也不得把 proxy 改善/损失直接解释为exposure-cap 可实盘使用。
