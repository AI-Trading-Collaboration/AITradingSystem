# Risk-Off Risk-On Timing Quality Review

- 状态：`RISK_TIMING_QUALITY_REVIEW_READY_WITH_BLOCKERS`
- market_regime：`ai_after_chatgpt`
- date_range：`2022-12-01` to `2026-06-26`
- data_quality_status：`PASS_WITH_WARNINGS`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Timing Verdicts

|strategy_id|risk_off_count|risk_on_count|avoided_loss|false_cost|missed_upside|verdict|
|---|---|---|---|---|---|---|
|limited_adjustment|6|6|0.015679|0.043247|0.038507|RISK_OFF_TOO_NOISY|
|dynamic_v0_5_ai_trend_confirmed_only|18|17|0.071985|0.159973|0.210883|RISK_OFF_TOO_NOISY|
|limited_adjustment_event_override_v1|9|15|0.066815|0.086494|0.053353|RISK_OFF_TOO_NOISY|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1|27|42|0.126523|0.311764|0.222262|RISK_OFF_TOO_NOISY|

## Gate 结论

Timing quality 只读取 actual-path position path。若 risk-off 太吵、risk-on 太慢或证据不足，候选仍不得进入 paper-shadow preflight。
