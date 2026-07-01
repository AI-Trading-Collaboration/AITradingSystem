# Event Calendar Data Feasibility Audit

TRADING-2318 只审计 event calendar source 的 PIT / known-at 可行性。它不下载事件 rows，不生成 gating signal，不预测事件结果，不进入仓位、paper-shadow、production 或 broker path。

- status: `EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `static_feasibility_audit`
- data_quality_status: `NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT`
- event_source_count: `8`
- source_audit_required_count: `8`
- pit_ready_source_count: `0`
- source_blocked_count: `4`
- gating_use_case_count: `4`
- generator_poc_ready_now: `False`
- recommended_next_task: `TRADING-2319_EVENT_CALENDAR_GATING_GENERATOR_POC_AFTER_SOURCE_AUDIT`
- gating_signal_generated: `False`
- event_outcome_prediction_allowed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Source Inventory

|source_id|event_family|provider_class|pit_status|recommended_usage|
|---|---|---|---|---|
|`ai_mega_cap_earnings_calendar`|`AI_MEGA_CAP_EARNINGS`|`primary_or_paid_vendor_required`|`BLOCKED_PENDING_AS_KNOWN_BEFORE_EARNINGS_SOURCE`|earnings_cluster_manual_review_after_source_manifest|
|`cpi_release_calendar`|`CPI`|`primary_source`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|inflation_event_gating_after_source_manifest|
|`fomc_calendar`|`FOMC`|`primary_source`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|macro_event_gating_after_source_manifest|
|`nfp_release_calendar`|`NFP`|`primary_source`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|labor_event_gating_after_source_manifest|
|`nvda_earnings_calendar`|`NVDA_EARNINGS`|`primary_or_paid_vendor_required`|`BLOCKED_PENDING_AS_KNOWN_BEFORE_EARNINGS_SOURCE`|manual_review_context_after_as_known_source|
|`pce_release_calendar`|`PCE`|`primary_source`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|inflation_event_gating_after_source_manifest|
|`semiconductor_earnings_window`|`SEMICONDUCTOR_EARNINGS_WINDOW`|`composite_source_policy_required`|`BLOCKED_PENDING_UNIVERSE_AND_AS_KNOWN_SOURCE`|earnings_cluster_risk_after_universe_policy_and_source_manifest|
|`tsm_monthly_revenue_calendar`|`TSM_MONTHLY_REVENUE`|`primary_source`|`BLOCKED_PENDING_RELEASE_TIME_ARCHIVE`|semiconductor_manual_review_context_after_source_manifest|

## Gating Use Cases

|use_case_id|runtime_status|blocked_event_families|
|---|---|---|
|`earnings_cluster_risk`|`BLOCKED_PENDING_UNIVERSE_POLICY_AND_SOURCE_MANIFEST`|NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS,TSM_MONTHLY_REVENUE,SEMICONDUCTOR_EARNINGS_WINDOW|
|`manual_review_trigger`|`BLOCKED_PENDING_MANUAL_REVIEW_CONTRACT`|FOMC,CPI,PCE,NFP,NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS,TSM_MONTHLY_REVENUE|
|`post_event_confirmation_window`|`BLOCKED_PENDING_TRADING_2319_GENERATOR_POC`|FOMC,CPI,PCE,NFP,NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS|
|`pre_event_no_add`|`BLOCKED_PENDING_TRADING_2319_GENERATOR_POC`|FOMC,CPI,PCE,NFP,NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS|

## Boundary

当前所有 use case 都是 source feasibility / design only。TRADING-2319 之前必须先补 provider-specific source manifest、event row schema validation、known-at / available-at timestamp、row count 和 checksum。任何 no-add、manual review、post-event confirmation、scoring、report、paper-shadow、production 或 broker 使用都需要独立任务和 owner review。
