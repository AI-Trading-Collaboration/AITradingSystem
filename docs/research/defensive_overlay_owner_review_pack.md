# Defensive Overlay Owner Review Pack

- Status: `DEFENSIVE_OVERLAY_OWNER_REVIEW_PACK_READY_PROMOTION_BLOCKED`
- Owner recommendation: `KEEP_DEFENSIVE_OVERLAY_RESEARCH_ONLY_WATCH_PENDING_SPLIT_EVIDENCE`
- Full allocation survivor count: `0`
- Primary watch count: `1`
- Overlay gate pass count: `0`
- Dynamic promotion: `BLOCKED`
- Paper-shadow allowed: `False`
- Production allowed: `False`
- Broker action: `none`

## Owner Interpretation

- Full allocation gate 继续 blocked，因为 static frontier、net-of-cost 和 walk-forward blockers 仍未解除。
- Defensive overlay gate 目前最多只有 watch-only research 价值；当前 rows 因 split evidence pending 不能 pass。
- `limited_adjustment` 是 pilot overlay tolerance 下唯一 primary watch row，且只能 research-only / watch-only。
- 含 TQQQ 的 overlay rows 继续 diagnostic-only，直到单独的 TQQQ safety 和 stress review 获得 owner 复核。

## Candidate Matrix

|candidate_id|classification|gate_passed|allowed_use|blockers|
|---|---|---|---|---|
|expanded_state_highest_return_under_max_dd_cap|TQQQ_OVERLAY_DIAGNOSTIC_RESEARCH_ONLY|False|tqqq_research_diagnostic_only|stress_risk_too_high; overlay_stress_watch_cap_exceeded|
|expanded_state_lowest_turnover|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|risk_control_diagnostic_only||
|expanded_state_highest_calmar|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|risk_control_diagnostic_only||
|expanded_state_highest_sharpe|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|risk_control_diagnostic_only||
|expanded_state_lowest_drawdown|DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER|False|risk_control_diagnostic_only||
|limited_adjustment|DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE|False|research_overlay_watch_only|walk_forward_split_evidence_pending|
|static_simplex_qqq0150_sgov0850_tqqq0000|STATIC_REFERENCE_ONLY|False|static_reference_only||
|static_simplex_qqq0000_sgov0950_tqqq0050|STATIC_REFERENCE_ONLY|False|static_reference_only||
|static_simplex_qqq0100_sgov0900_tqqq0000|STATIC_REFERENCE_ONLY|False|static_reference_only||
|static_simplex_qqq0050_sgov0950_tqqq0000|STATIC_REFERENCE_ONLY|False|static_reference_only||
|static_simplex_qqq0000_sgov1000_tqqq0000|STATIC_REFERENCE_ONLY|False|static_reference_only||
