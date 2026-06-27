# Dynamic Actual-Path Owner Review Decision

- 状态：`DYNAMIC_ACTUAL_PATH_OWNER_REVIEW_DECISION_READY`
- market_regime：`ai_after_chatgpt`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- owner_manual_review_required：`true`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Candidate Decisions

|strategy_id|recommendation|owner_decision|annual_return|sharpe|lag|staleness|
|---|---|---|---|---|---|---|
|limited_adjustment|WATCH_ONLY|pending|0.192658|1.609148|PASS|PASS|
|dynamic_v0_5_ai_trend_confirmed_only|WATCH_ONLY|pending|0.182386|1.634858|PASS|PASS|

## Decision Notes

本报告只记录 system review recommendation 与 pending owner decision 字段。任何 `PAPER_SHADOW_CANDIDATE` 都不是 promotion，也不会自动进入 paper-shadow。

Target-path metrics 仅用于 target-vs-actual gap、execution lag 和 signal staleness diagnostic，不作为 owner decision 或 promotion readiness 的正向依据。
