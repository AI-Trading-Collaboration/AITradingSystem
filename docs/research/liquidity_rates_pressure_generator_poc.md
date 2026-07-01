# Liquidity / Rates Pressure Generator POC

TRADING-2312 生成 research-only partial rates-only candidate-bound artifacts。

- status: `LIQUIDITY_RATES_PRESSURE_GENERATOR_POC_READY_VALIDATION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- generated_source_date_range: `2022-12-01..2026-06-26`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_quality_report_path: `D:\Work\AITradingSystem\outputs\research_trends\liquidity_rates_pressure_generator_poc\data_quality_2026-06-29.md`
- policy_version: `liquidity_rates_pressure_generator_policy:v1`
- partial_rates_only_generator_poc: `True`
- full_liquidity_pressure_poc_ready: `False`
- liquidity_headwind_generator_implemented: `False`
- actual_path_validation_ready: `False`

## Generated Candidates

|candidate_id|signal_record_count|validation_status|actual_path_validation_ready|
|---|---:|---|---|
|`duration_pressure_proxy_v1`|`15984`|`PASS`|`False`|
|`rates_pressure_exposure_cap_modifier_v1`|`15984`|`PASS`|`False`|

## Blocked Candidates

|candidate_id|missing_inputs|blocker|
|---|---|---|
|`liquidity_headwind_proxy_v1`|`UUP_or_DXY_price_proxy,HYG_price_proxy,LQD_price_proxy`|`SOURCE_GAP_BLOCKED_BY_TRADING_2311`|

## Safety

本 POC 只使用 TLT / SHY / DGS10 / DGS2 partial rates route；`liquidity_headwind_proxy_v1` 因 UUP / HYG / LQD source gap 不生成 candidate-bound artifacts。当前不得用于 actual-path validation、scope review、promotion、paper-shadow、production 或 broker action。
