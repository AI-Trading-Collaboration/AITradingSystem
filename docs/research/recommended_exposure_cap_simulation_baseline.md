# Recommended Exposure-Cap Simulation Baseline

- status: `PORTFOLIO_BASELINE_SOURCE_DECISION_READY_PROMOTION_BLOCKED`
- selected_for_2326: `static_etf_allocation_baseline`
- fallback_baseline: `synthetic_observe_only_baseline`
- baseline_source_mode: `static_etf_allocation`
- pit_status: `PIT_APPROXIMATION_READY`
- reproducibility_status: `REPLAYABLE_CONFIG_VERSIONED`
- allowed_simulation_mode: `source_bound_dry_run_only`
- next_task: `TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Static_ETF_Baseline`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

TRADING-2326 应先使用 static ETF allocation baseline 做 source-bound exposure-cap dry-run；synthetic observe-only baseline 只作为 fallback。dynamic target baseline 保留为中期路线，actual holdings-derived baseline 仅能作为 future owner-only manual reference。
