# Expanded Universe Walk-Forward Split Evidence

- Status: `WALK_FORWARD_SPLIT_EVIDENCE_PENDING_PROMOTION_BLOCKED`
- Market regime: `ai_after_chatgpt`
- Dynamic promotion: `BLOCKED`
- Paper-shadow allowed: `False`
- Production allowed: `False`
- Broker action: `none`

## Summary

- market_regime: `ai_after_chatgpt`
- default_backtest_start: `2022-12-01`
- candidate_count: `11`
- walk_forward_pending_count: `11`
- overlay_gate_pass_count_after_split_evidence: `0`

## Candidate Matrix

|candidate_id|classification|gate_passed|drawdown_improvement|annual_return_edge|net_edge|next_action|
|---|---|---|---|---|---|---|
|expanded_state_highest_return_under_max_dd_cap|TQQQ_OVERLAY_DIAGNOSTIC_RESEARCH_ONLY|False|0.08512|-0.025803|-0.025803|REQUIRE_TQQQ_SAFETY_AND_STRESS_REVIEW_BEFORE_ANY_WATCH_USE|
|expanded_state_lowest_turnover|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|0.097234|-0.033346|-0.033346|KEEP_AS_RISK_CONTROL_DIAGNOSTIC_ONLY|
|expanded_state_highest_calmar|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|0.095539|-0.036138|-0.036138|KEEP_AS_RISK_CONTROL_DIAGNOSTIC_ONLY|
|expanded_state_highest_sharpe|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|0.095602|-0.036289|-0.036289|KEEP_AS_RISK_CONTROL_DIAGNOSTIC_ONLY|
|expanded_state_lowest_drawdown|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|0.095653|-0.038162|-0.038162|KEEP_AS_RISK_CONTROL_DIAGNOSTIC_ONLY|
|limited_adjustment|DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE|False|0.022741|-0.004903|-0.004903|COLLECT_WALK_FORWARD_SPLIT_AND_FORWARD_WATCH_EVIDENCE|
|static_simplex_qqq0150_sgov0850_tqqq0000|STATIC_REFERENCE_ONLY|False|-0.001095|0.001816|0.001816|KEEP_AS_STATIC_REFERENCE_NOT_OVERLAY|
|static_simplex_qqq0000_sgov0950_tqqq0050|STATIC_REFERENCE_ONLY|False|-0.001357|0.000676|0.000676|KEEP_AS_STATIC_REFERENCE_NOT_OVERLAY|
|static_simplex_qqq0100_sgov0900_tqqq0000|STATIC_REFERENCE_ONLY|False|-0.000225|0.000217|0.000217|KEEP_AS_STATIC_REFERENCE_NOT_OVERLAY|
|static_simplex_qqq0050_sgov0950_tqqq0000|STATIC_REFERENCE_ONLY|False|-0.000119|0.000112|0.000112|KEEP_AS_STATIC_REFERENCE_NOT_OVERLAY|
|static_simplex_qqq0000_sgov1000_tqqq0000|STATIC_REFERENCE_ONLY|False|0.0|0.0|0.0|KEEP_AS_STATIC_REFERENCE_NOT_OVERLAY|

解释：该 review 只用于 defensive overlay research-only 诊断，不构成 allocation promotion。
