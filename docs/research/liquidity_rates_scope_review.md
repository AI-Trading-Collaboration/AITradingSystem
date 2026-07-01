# Liquidity / Rates Scope Review

TRADING-2314 对 TRADING-2313 actual-path validation evidence 执行 research-only scope review。

- status: `LIQUIDITY_RATES_SCOPE_REVIEW_DIAGNOSTIC_ONLY`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- data_quality_status: `PASS_WITH_WARNINGS`
- source_status: `LIQUIDITY_RATES_VALIDATED_INCONCLUSIVE`
- scope_review_result: `DIAGNOSTIC_ONLY_WITH_LIMITED_RISK_CAP_RESEARCH_CANDIDATE`
- recommended_use_cases: `risk_cap_modifier,max_exposure_limiter,diagnostic_only`
- diagnostic_use_cases: ``
- preferred_owner_review_horizons: `10d`
- diagnostic_owner_review_horizons: `20d,1m`
- liquidity_headwind_scope_review_executed: `False`
- forward_observe_started: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- dynamic_promotion_status: `BLOCKED`

## Recommended Scope

- scope_review_result: `DIAGNOSTIC_ONLY_WITH_LIMITED_RISK_CAP_RESEARCH_CANDIDATE`
- recommended_candidate_ids: `duration_pressure_proxy_v1`
- diagnostic_candidate_ids: `rates_pressure_exposure_cap_modifier_v1`
- not_recommended_as: `broker_action,full_liquidity_headwind,no_add_gate,paper_shadow,production,scope_ready_research_only,standalone_alpha`

## Candidate Scope

|scope_id|eligible_record_count|average_alignment_score|scope_decision|
|---|---|---|---|
|duration_pressure_proxy_v1|15624|0.044616|KEEP_RESEARCH_SCOPE|
|rates_pressure_exposure_cap_modifier_v1|15624|0.027778|DIAGNOSTIC_ONLY|

## Horizon Scope

|scope_id|eligible_record_count|average_alignment_score|scope_decision|
|---|---|---|---|
|10d|10536|0.057509|KEEP_RESEARCH_SCOPE|
|20d|10416|0.021681|DIAGNOSTIC_ONLY|
|1m|10296|0.029073|DIAGNOSTIC_ONLY|

## Use-Case Scope

|scope_id|eligible_record_count|average_alignment_score|scope_decision|
|---|---|---|---|
|risk_cap_modifier|15624|0.192124|KEEP_RESEARCH_SCOPE|
|no_add_gate|15624|-0.061956|REJECT_CURRENT_SCOPE|
|max_exposure_limiter|10416|0.169211|KEEP_RESEARCH_SCOPE|
|diagnostic_only|31248|0.036197|KEEP_RESEARCH_SCOPE|

## Safety

`liquidity_headwind_proxy_v1` 因 UUP / HYG / LQD source gap 没有 TRADING-2313 validation rows，本报告不得为该 route 生成 scope row。本报告不修改 generator 或 actual-path validation artifacts，不启动 forward observe，不允许 promotion、paper-shadow、production 或 broker action。
