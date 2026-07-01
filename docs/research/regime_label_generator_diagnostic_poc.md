# Regime Label Generator Diagnostic POC

TRADING-2316 生成 diagnostic-only regime label series，供后续 validation segmentation 使用；它不是交易信号，也不进入仓位、日报生产建议或 broker path。

- status: `REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- data_quality_status: `PASS_WITH_WARNINGS`
- policy_version: `v1`
- label_row_count: `5370`
- distribution_row_count: `30`
- transition_row_count: `119`
- observed_label_ids: `drawdown,failed_rebound,high_volatility,late_uptrend,low_volatility,normal_volatility,panic,range_bound,rebound,uptrend`
- segmentation_ready_scope: `diagnostic_only_validation_segmentation`
- candidate_signal_generated: `False`
- candidate_artifact_generated: `False`
- actual_path_validation_executed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## PIT Boundary

Labels only use trailing adjusted-close features known after each market close. The POC blocks future return, future drawdown, future volatility, final peak/trough and hindsight episode relabeling. `normal_volatility` is an overlay neutral state, not a TRADING-2315 taxonomy label.

## Data Quality

Cached data quality is enforced through the same validation code path as `aits validate-data`; the generated summary and PIT policy link the data quality report and disclose the status.

## Safety

本产物只能作为 TRADING-2317 segmentation input。任何 candidate validation、scope review、forward observe、report integration、paper-shadow、production 或 broker 使用都需要独立任务、独立质量门禁和 owner review。
