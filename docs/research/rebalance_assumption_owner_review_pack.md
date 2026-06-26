# Rebalance Assumption Owner Review Pack

- 状态：`REBALANCE_ASSUMPTION_NEEDS_OWNER_DECISION`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`
- manual_review_required：`true`

## Required Answers

|Question|Answer|
|---|---|
|`current_implicit_monthly_execution`|`dynamic strategy families contain explicit monthly fields but no execution_policy_id contract`|
|`conclusions_only_under_monthly`|`balanced core, controlled growth and Layer-1 selector conclusions need execution-policy review`|
|`execution_frequency_sensitive_strategies`|`equal-risk and balanced-core require hybrid sensitivity before defaults`|
|`monthly_killed_candidates`|`candidate recovery review found families to reopen for execution-semantics replay`|
|`equal_risk_default_policy`|`monthly_plus_threshold_5pct_v1 pending owner review`|
|`balanced_core_default_policy`|`monthly_plus_vol_shock_v1 pending rebacktest and owner review`|
|`forward_aging_upgrade`|`yes, future observations must be execution-aware`|

本报告仅用于 research-only owner review，不生成交易建议、paper-shadow activation、production config mutation 或 broker action。
