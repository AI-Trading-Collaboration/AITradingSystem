# Portfolio Baseline Source Decision

TRADING-2325 只回答后续 exposure-cap simulation 应绑定哪一种 portfolio / exposure baseline source。TRADING-2324 已达到 `SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`，但 synthetic observe-only baseline 不能被解释为真实组合层 simulation 结论。

- status: `PORTFOLIO_BASELINE_SOURCE_DECISION_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- source_binding_dry_run_readiness_status: `SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`
- source_binding_portfolio_source_mode: `synthetic_observe_only`
- data_quality_status: `NOT_APPLICABLE_SOURCE_DECISION_ONLY`
- aits_validate_data_executed: `False`
- selected_baseline_for_2326: `static_etf_allocation_baseline`
- fallback_baseline: `synthetic_observe_only_baseline`
- next_task: `TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Static_ETF_Baseline`
- simulation_executed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Data Quality

`aits validate-data` 不适用：本任务只读取 static config 和 prior research outputs。TRADING-2326 如果消费 cached market data，必须重新执行 cached-data quality gate。

## Baseline Candidates

|baseline|source_available|dry_run_eligible|pit_status|recommended_usage|
|---|---|---|---|---|
|`synthetic_observe_only_baseline`|`True`|`True`|`SYNTHETIC_OBSERVE_ONLY`|early dry-run, source binding smoke test, mechanics verification|
|`static_etf_allocation_baseline`|`True`|`True`|`PIT_APPROXIMATION_READY`|first source-bound dry-run simulation and exposure-cap vs no-cap comparison|
|`dynamic_strategy_target_exposure_baseline`|`False`|`False`|`BLOCKED`|post-static-baseline validation after PIT target exposure artifacts exist|
|`paper_portfolio_advisory_baseline`|`True`|`False`|`REPLAYABLE_BUT_NOT_STRICT_PIT`|forward observe simulation and paper advisory comparison|
|`actual_holdings_derived_baseline`|`False`|`False`|`BLOCKED`|not recommended for current stage|

## Recommendation

短期推荐 `static_etf_allocation_baseline`，因为它比 synthetic baseline 更接近组合研究，同时仍可复现、隐私风险低、工程成本低。中期应补 `dynamic_strategy_target_exposure_baseline` 的 PIT target exposure artifact。`actual_holdings_derived_baseline` 当前不推荐，因为会把研究层与真实账户、现金、税务和 broker 执行边界混在一起。

- selected_for_2326: `static_etf_allocation_baseline`
- baseline_source_mode: `static_etf_allocation`
- pit_status: `PIT_APPROXIMATION_READY`
- route: `TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Static_ETF_Baseline`
