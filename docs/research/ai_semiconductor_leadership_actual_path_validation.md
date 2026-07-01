# AI / 半导体 Leadership Actual-Path Validation

TRADING-2309 对 TRADING-2308 candidate-bound price-proxy artifacts 执行 research-only actual-path validation。

- status: `AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_quality_report_path: `D:\Work\AITradingSystem\outputs\research_trends\ai_leadership_actual_path_validation\data_quality_2026-06-29.md`
- full_universe_readiness_claimed: `False`
- full_universe_validation_blocker_out_of_scope: `full_universe_validation_blocked_by_ASX_missing_out_of_scope`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- dynamic_promotion_status: `BLOCKED`

## Candidate Scorecards

|candidate_id|eligible|avg_alignment|status|
|---|---:|---:|---|
|`smh_relative_strength_leadership_v1`|15882|0.048105|`AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`|
|`ai_semiconductor_leadership_quality_v1`|15882|0.032474|`AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`|
|`ai_core_basket_leadership_v1`|15882|0.055026|`AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`|

## Objective Coverage

|objective_id|eligible|avg_alignment|status|
|---|---:|---:|---|
|`smh_future_relative_return`|23823|-0.113588|`INCONCLUSIVE_OR_WEAK`|
|`qqq_smh_drawdown_risk`|47646|0.10005|`PASS`|
|`ai_leadership_weakening_windows`|18490|0.035262|`INCONCLUSIVE_OR_WEAK`|
|`smh_overweight_risk`|5294|0.057423|`PASS`|

## Safety

本报告只验证 actual-path evidence，不修改 TRADING-2308 generator artifacts，不执行 scope review，不允许 promotion、paper-shadow、production 或 broker action。
