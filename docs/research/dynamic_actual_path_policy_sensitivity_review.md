# Dynamic Actual-Path Policy Sensitivity Review

- 状态：`POLICY_SENSITIVITY_REVIEW_READY`
- market_regime：`ai_after_chatgpt`
- matrix_mode：`staged`
- Stage A：`execution_lag_days x rebalance_frequency`
- Stage B：`signal_validity_window_days x turnover_constraint on lag=1 weekly/monthly`
- ranking_basis：`actual_path annual_return, max_drawdown, sharpe, calmar, turnover`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- owner_manual_review_required：`true`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Classification

|strategy_id|classification|surviving|tested|next_action|failure_modes|
|---|---|---|---|---|---|
|limited_adjustment|POLICY_SENSITIVE_BUT_WATCHABLE|18|37|WATCH_ONLY|SIGNAL_STALENESS_COST_MATERIAL;STATIC_BASELINE_UNDERPERFORMANCE|
|dynamic_v0_5_ai_trend_confirmed_only|POLICY_SENSITIVE_BUT_WATCHABLE|14|37|WATCH_ONLY|SIGNAL_STALENESS_COST_MATERIAL;STATIC_BASELINE_UNDERPERFORMANCE|

## Policy

A scenario survives when actual_path annual_return is above same-scenario no_trade and lag/staleness materiality is not FAIL.

Target-path metrics 只用于解释 target-vs-actual gap、execution lag cost 和 signal staleness cost，不参与 policy sensitivity ranking 或 next action。
