# Liquidity / Rates Pressure Data Feasibility Audit

TRADING-2311 只审计 liquidity / rates pressure 输入可行性，不生成 candidate-bound artifacts。

- status: `LIQUIDITY_RATES_FEASIBILITY_AUDIT_READY_PARTIAL_PROXY`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- data_quality_status: `PASS_WITH_WARNINGS`
- available_price_proxy_count: `4`
- missing_price_proxy_symbols: `IEF,UUP,HYG,LQD`
- available_macro_series_count: `3`
- missing_macro_series: `DFII10,T10YIE,SOFR`
- partial_poc_possible: `True`
- full_liquidity_pressure_poc_ready: `False`
- recommended_next_task: `TRADING-2312_LIQUIDITY_RATES_PRESSURE_GENERATOR_POC_PARTIAL_RATES_ONLY`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- dynamic_promotion_status: `BLOCKED`

## Rates Proxy Inventory

|input_id|source_status|available_dependencies|missing_dependencies|candidate_id|
|---|---|---|---|---|
|`duration_pressure_proxy`|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|`TLT,SHY,DGS10,DGS2`|``|`duration_pressure_proxy_v1`|
|`intermediate_treasury_proxy`|`BLOCKED_MISSING_PRICE_PROXY`|``|`IEF`|`duration_pressure_proxy_v1`|
|`usd_liquidity_proxy`|`PARTIAL_MACRO_PROXY_ONLY_PRICE_PROXY_MISSING`|`DTWEXBGS`|`UUP`|`liquidity_headwind_proxy_v1`|
|`credit_liquidity_proxy`|`BLOCKED_MISSING_CREDIT_ETF_PROXIES`|``|`HYG,LQD`|`liquidity_headwind_proxy_v1`|
|`real_rate_proxy`|`BLOCKED_MISSING_REAL_RATE_SERIES`|``|`DFII10,T10YIE`|`rates_pressure_exposure_cap_modifier_v1`|
|`qqq_smh_valuation_pressure_context`|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|`QQQ,SMH,TLT,DGS10,DGS2`|``|`rates_pressure_exposure_cap_modifier_v1`|

## Price Proxy Coverage

|symbol|row_count|history_start|history_end|source_status|
|---|---:|---|---|---|
|`TLT`|2133|2018-01-02|2026-06-29|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|
|`IEF`|0|||`SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE`|
|`SHY`|2133|2018-01-02|2026-06-29|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|
|`UUP`|0|||`SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE`|
|`HYG`|0|||`SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE`|
|`LQD`|0|||`SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE`|
|`QQQ`|2133|2018-01-02|2026-06-29|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|
|`SMH`|2133|2018-01-02|2026-06-29|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|

## Macro Rates Coverage

|series|row_count|history_start|history_end|source_status|known_at_status|
|---|---:|---|---|---|---|
|`DGS10`|2121|2018-01-02|2026-06-26|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|`release_timestamp_not_cached_observation_date_only`|
|`DGS2`|2121|2018-01-02|2026-06-26|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|`release_timestamp_not_cached_observation_date_only`|
|`DTWEXBGS`|2117|2018-01-02|2026-06-26|`AVAILABLE_AFTER_DATA_QUALITY_GATE`|`release_timestamp_not_cached_observation_date_only`|
|`DFII10`|0|||`SOURCE_GAP_MISSING_LOCAL_RATE_CACHE`|`missing_source`|
|`T10YIE`|0|||`SOURCE_GAP_MISSING_LOCAL_RATE_CACHE`|`missing_source`|
|`SOFR`|0|||`SOURCE_GAP_MISSING_LOCAL_RATE_CACHE`|`missing_source`|

## Validation Route

|candidate_id|readiness_status|allowed_next_step|blocked_validation|
|---|---|---|---|
|`duration_pressure_proxy_v1`|`PARTIAL_GENERATOR_POC_READY_AFTER_DQ`|`TRADING-2312_PARTIAL_RATES_GENERATOR_POC`|actual_path_validation; scope_review; promotion|
|`liquidity_headwind_proxy_v1`|`SOURCE_GAP_BLOCKED`|`source_gap_resolution_before_full_generator`|generator_poc_full_scope; actual_path_validation; promotion|
|`rates_pressure_exposure_cap_modifier_v1`|`PARTIAL_GENERATOR_POC_READY_AFTER_DQ`|`TRADING-2312_PARTIAL_RATES_GENERATOR_POC`|actual_path_validation; scope_review; promotion|

## Safety

本报告只记录输入可行性和 source gap。缺失的 IEF / UUP / HYG / LQD / real-rate series 不得被平滑成可用输入；当前不允许 generator、actual-path validation、scope review、promotion、paper-shadow、production 或 broker action。
