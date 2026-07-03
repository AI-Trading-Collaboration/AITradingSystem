# Dynamic Exposure-Cap vs No-Cap Diagnostics Review

TRADING-2333 只读取 TRADING-2332 已验证的 dynamic target baseline dry-run artifacts，并把 TRADING-2327 / 2326 static reference 作为对照。本任务不重新执行 simulation，不修改 policy，不生成交易指令。

- status: `DYNAMIC_EXPOSURE_CAP_DIAGNOSTICS_REVIEW_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_validation_policy: `NOT_APPLICABLE_PRIOR_VALIDATED_DYNAMIC_DRY_RUN_ARTIFACTS_ONLY`
- known_at_policy: `NEXT_SESSION_DECISION_POLICY`
- strict_pit_ready: `False`
- cap_binding_rate: `0.455422`
- cap_binding_frequency_label: `EXCESSIVE_BINDING_FREQUENCY`
- overbinding_label: `OVERBINDING_BLOCKING`
- return_proxy_delta: `-0.187258`
- drawdown_proxy_delta: `0.045294`
- return_drawdown_tradeoff_label: `DRAWDOWN_PROTECTION_WEAK_RETURN_COST_HIGH`
- false_cost_label: `FALSE_COST_BLOCKING`
- missed_upside_label: `MISSED_UPSIDE_BLOCKING`
- downside_protection_label: `DOWNSIDE_PROTECTION_POSITIVE_PROXY`
- dynamic_strategy_overlap_label: `RISK_CAP_BINDING_WHEN_DYNAMIC_MISSES_RISK`
- static_vs_dynamic_comparison_label: `DYNAMIC_BASELINE_INCONCLUSIVE`
- overall_recommendation: `HIGH_INTENSITY_ONLY_FORWARD_OBSERVE`
- next_task: `TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## 解释边界

Dynamic wrapper 使用 `NEXT_SESSION_DECISION_POLICY`，不是 strict PIT。这些 diagnostics 不能解释为真实账户表现、真实仓位建议、paper-shadow signal、production strategy 或 broker action。
