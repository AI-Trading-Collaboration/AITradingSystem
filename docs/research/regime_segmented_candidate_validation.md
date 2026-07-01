# Regime-Segmented Candidate Validation

TRADING-2317 用 TRADING-2316 的 regime labels 重新解释已有候选 actual-path evidence。它不生成新 signal，不改变候选 verdict，不进入仓位、paper-shadow、production 或 broker path。

- status: `REGIME_SEGMENTED_CANDIDATE_VALIDATION_READY_DIAGNOSTIC_ONLY`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `2022-12-01..2026-06-29`
- data_quality_status: `PASS_WITH_WARNINGS`
- label_source_status: `REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY`
- performance_row_count: `294`
- coverage_row_count: `31`
- family_blocker_row_count: `4`
- segmentable_families: `volatility_risk_cap,ai_leadership,liquidity_pressure`
- blocked_families: `breadth_proxy`
- candidate_signal_generated: `False`
- existing_candidate_verdict_changed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Boundary

Segment metrics are diagnostic only. Breadth proxy remains source-blocked because current constituent snapshots are missing. Any future use in report integration, forward observe, scope review, paper-shadow, production or broker paths requires a separate owner-reviewed task and quality gate.
