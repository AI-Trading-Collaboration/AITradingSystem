# Breadth Proxy Signal Concept Selection

TRADING-2304 只做 diagnostics-only signal concept selection。

- status: `BREADTH_PROXY_SIGNAL_SELECTION_SOURCE_BLOCKED_NO_SELECTION`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `source_blocked_signal_concept_selection`
- data_quality_status: `NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_SELECTION`
- source_status: `CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED`
- selected_concept_count: `0`
- rejected_concept_count: `7`
- advance_to_generator_allowed: `False`
- recommended_next_action: `REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT_BEFORE_SELECTION`

## Selection Result

当前没有 signal concept 被选入 TRADING-2305。原因不是 signal quality 失败，而是 TRADING-2303 source-blocked，无法计算分布、neutrality、asset concentration、trend fragility 或 bias acceptance evidence。

## Scorecard

|signal_name|selection_decision|distribution|neutrality|bias|
|---|---|---|---|---|
|`breadth_participation_score`|`REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|
|`advance_decline_participation_score`|`REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|
|`constituent_momentum_breadth_score`|`REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|
|`new_high_new_low_proxy_score`|`REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|
|`mega_cap_concentration_risk_score`|`REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|
|`sector_leadership_diffusion_score`|`REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|
|`trend_fragility_score`|`REJECTED_SOURCE_BLOCKED_NOT_SIGNAL_QUALITY_REJECTION`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|`NOT_EVALUATED_SOURCE_BLOCKED`|

## Selected Concepts

- selected_concept_count: `0`

## Rejected Concepts

- rejected_concept_count: `7`
- rejection_scope_note: Concepts are rejected from TRADING-2305 now because source diagnostics are missing, not because the concepts failed a measured distribution test.

## Safety

selection_status=`source_blocked_no_selection`, advance_to_generator_allowed=`False`, promotion_allowed=`False`, paper_shadow_allowed=`False`, production_allowed=`False`, broker_action=`none`, candidate_artifact_generated=`False`, actual_path_validation_executed=`False`.

本报告不得用于 candidate generation、actual-path validation、promotion、paper-shadow、production 或 broker action。
