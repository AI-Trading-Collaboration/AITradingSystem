# Breadth Participation 2303 Task Route

- next_task: `TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only`
- caveat: `SURVIVORSHIP_BIAS`
- generator_implementation_allowed_now: `False`
- diagnostics_only_allowed_now: `True`

|candidate_id|data_mode|pit_status|allowed_validation|next_task|
|---|---|---|---|---|
|`qqq_breadth_participation_v1`|`strict_pit_historical_constituents`|`BLOCKED_NO_RELIABLE_DATA`|`research_design_only`|`TRADING-2306_Breadth_Data_Source_Investment_Decision`|
|`smh_breadth_participation_v1`|`current_constituents_proxy`|`CURRENT_CONSTITUENTS_PROXY_ONLY`|`candidate_generator_poc`|`TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only`|
|`cross_asset_breadth_quality_v1`|`pit_approximation_historical_constituents`|`BLOCKED_NO_RELIABLE_DATA`|`offline_diagnostics`|`TRADING-2306_Breadth_Data_Source_Investment_Decision`|
|`mega_cap_concentration_fragility_v1`|`forward_only`|`FORWARD_OBSERVE_ONLY`|`forward_observe_only`|`TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only`|

当前推荐进入 current constituents proxy diagnostics-only 路线。这不是 generator implementation approval，也不是 actual-path validation approval。
