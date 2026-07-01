# Liquidity / Rates Actual-Path Validation

TRADING-2313 对 TRADING-2312 partial rates-only candidate-bound artifacts 执行 research-only actual-path validation。

- status: `LIQUIDITY_RATES_VALIDATED_INCONCLUSIVE`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_quality_report_path: `D:\Work\AITradingSystem\outputs\research_trends\liquidity_rates_actual_path_validation\data_quality_2026-06-29.md`
- partial_rates_only_validation: `True`
- liquidity_headwind_validation_executed: `False`
- full_liquidity_pressure_validation_ready: `False`
- scope_review_ready: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- dynamic_promotion_status: `BLOCKED`

## Candidate Scorecards

|candidate_id|eligible|avg_alignment|status|
|---|---:|---:|---|
|`duration_pressure_proxy_v1`|15624|0.044616|`LIQUIDITY_RATES_VALIDATED_CONTINUE_RESEARCH`|
|`rates_pressure_exposure_cap_modifier_v1`|15624|0.027778|`LIQUIDITY_RATES_VALIDATED_INCONCLUSIVE`|

## Objective Coverage

|objective_id|eligible|avg_alignment|status|
|---|---:|---:|---|
|`qqq_smh_valuation_pressure`|31248|-0.049827|`INCONCLUSIVE_OR_WEAK`|
|`high_duration_asset_drawdown`|31248|-0.055748|`FAIL`|
|`risk_on_exposure_cap`|31248|0.214166|`PASS`|

## Horizon Coverage

|horizon|eligible|avg_alignment|status|
|---|---:|---:|---|
|`10d`|10536|0.057509|`PASS`|
|`20d`|10416|0.021681|`PASS`|
|`1m`|10296|0.029073|`PASS`|

## Safety

`liquidity_headwind_proxy_v1` 因 UUP / HYG / LQD source gap 没有 TRADING-2312 signal series，本报告不得为该 route 生成 validation rows。本报告不修改 generator artifacts，不执行 scope review，不允许 promotion、paper-shadow、production 或 broker action。
