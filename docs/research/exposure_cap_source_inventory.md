# Exposure-Cap Source Inventory

- status: `EXPOSURE_CAP_SIMULATION_SOURCE_BOUND_DRY_RUN_READY_PROMOTION_BLOCKED`
- data_quality_status: `PASS_WITH_WARNINGS`

|source_category|available|source_mode|data_quality_status|coverage|
|---|---|---|---|---|
|`risk_cap_trigger_series`|`True`|`TRADING_2291_scope_narrowed_candidate_signal_series`|`RESEARCH_OUTPUT_VALIDATED_BY_2292`|`2023-01-06..2026-06-18`|
|`market_price_history`|`True`|`validated_cached_market_price_history`|`PASS_WITH_WARNINGS`|`2023-01-06..2026-06-18`|
|`portfolio_baseline`|`True`|`synthetic_observe_only`|`NOT_APPLICABLE_SYNTHETIC_OBSERVE_ONLY_BASELINE`|`2023-01-06..2026-06-18`|
|`rebalance_calendar`|`True`|`market_trading_days`|`PASS_WITH_WARNINGS`|`2023-01-06..2026-06-18`|
|`turnover_assumption`|`True`|`absolute_final_exposure_change`|`POLICY_GOVERNED_PROXY_ASSUMPTION`|`2023-01-06..2026-06-18`|
|`cooldown_policy`|`True`|`source_binding_policy`|`POLICY_GOVERNED_DRY_RUN_COOLDOWN`|`2023-01-06..2026-06-18`|
|`exposure_cap_policy`|`True`|`source_binding_policy`|`POLICY_GOVERNED_DRY_RUN_CAP`|`2023-01-06..2026-06-18`|
|`simulation_calendar`|`True`|`risk_cap_trigger_market_overlap_calendar`|`PASS_WITH_WARNINGS`|`2023-01-06..2026-06-18`|

## Source Gaps

|source_category|gap_type|gap_severity|
|---|---|---|
|`risk_cap_trigger_series`|`NONE_FOR_DRY_RUN`|`NONE`|
|`market_price_history`|`NONE_FOR_DRY_RUN`|`NONE`|
|`portfolio_baseline`|`REAL_PORTFOLIO_BASELINE_MISSING_SYNTHETIC_OBSERVE_ONLY_USED`|`FULL_SIMULATION_BLOCKER`|
|`rebalance_calendar`|`NONE_FOR_DRY_RUN`|`NONE`|
|`turnover_assumption`|`NONE_FOR_DRY_RUN`|`NONE`|
|`cooldown_policy`|`NONE_FOR_DRY_RUN`|`NONE`|
|`exposure_cap_policy`|`NONE_FOR_DRY_RUN`|`NONE`|
|`simulation_calendar`|`NONE_FOR_DRY_RUN`|`NONE`|
