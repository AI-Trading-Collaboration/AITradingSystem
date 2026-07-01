# AI / 半导体 Leadership Scope Review

TRADING-2310 对 TRADING-2309 actual-path validation evidence 执行 research-only scope review。

- status: `AI_LEADERSHIP_SCOPE_REVIEW_READY_RESEARCH_ONLY`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- data_quality_status: `PASS_WITH_WARNINGS`
- source_status: `AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`
- recommended_asset_scope: `QQQ_PLUS_SMH_RESEARCH_ONLY`
- preferred_owner_review_horizons: `10d`
- diagnostic_owner_review_horizons: `20d`
- recommended_use_cases: `confirmation_only,exposure_cap_modifier`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- dynamic_promotion_status: `BLOCKED`

## Recommended Scope

- scope_review_result: `QQQ_PLUS_SMH_RESEARCH_ONLY_10d_CONFIRMATION_ONLY_EXPOSURE_CAP_MODIFIER_RESEARCH_ONLY`
- smh_only_scope_decision: `DIAGNOSTIC_ONLY`
- qqq_plus_smh_scope_decision: `KEEP_RESEARCH_SCOPE`
- not_recommended_as: `standalone_alpha,paper_shadow,production,broker_action,smh_only_primary_scope,20d_primary_scope`

## Asset Scope

|scope_id|eligible_record_count|average_alignment_score|scope_decision|
|---|---|---|---|
|qqq_only_diagnostic|23823|0.07966|REFERENCE_ONLY_NOT_OWNER_SCOPE|
|smh_only|23823|0.010742|DIAGNOSTIC_ONLY|
|qqq_plus_smh|47646|0.045201|KEEP_RESEARCH_SCOPE|

## Horizon Scope

|scope_id|owner_review_requested|average_alignment_score|scope_decision|
|---|---|---|---|
|5d|False|0.083198|REFERENCE_ONLY_NOT_OWNER_SCOPE|
|10d|True|0.038608|KEEP_RESEARCH_SCOPE|
|20d|True|0.013221|DIAGNOSTIC_ONLY|

## Use-Case Scope

|scope_id|eligible_record_count|average_alignment_score|scope_decision|
|---|---|---|---|
|confirmation_only|26470|0.063688|KEEP_RESEARCH_SCOPE|
|exposure_cap_modifier|47646|0.10005|KEEP_RESEARCH_SCOPE|
|standalone_alpha|23823|-0.113588|REJECT_CURRENT_SCOPE|

## Safety

本报告只做 scope review，不修改 TRADING-2308 generator artifacts 或 TRADING-2309 actual-path validation artifacts，不启动 forward observe，不允许 promotion、paper-shadow、production 或 broker action。
