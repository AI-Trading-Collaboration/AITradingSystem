# High-Intensity Risk-Cap Threshold Selection

TRADING-2335 承接 TRADING-2334 `THRESHOLD_SELECTION_REQUIRED` route，只做 deterministic threshold selection。本任务不启动 runtime observe，不生成 target weight、rebalance instruction、paper-shadow、production 或 broker action。

- status: `HIGH_INTENSITY_THRESHOLD_SELECTION_READY_WITH_WARNINGS_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_validation_policy: `NOT_APPLICABLE_PRIOR_VALIDATED_2332_2333_2334_ARTIFACTS_ONLY`
- aits validate-data: `not applicable`，因为本任务只读取 prior validated research artifacts。
- threshold_selection_status: `THRESHOLD_SELECTED_WITH_WARNINGS_PROMOTION_BLOCKED`
- selected_threshold_id: `COMPOSITE_HIGH_INTENSITY_RULE`
- selected_threshold_density: `0.06747`
- density_guardrail_status: `PASS_WITH_WARNINGS`
- readiness_status: `READY_FOR_2336_EVENT_LOGGER_WITH_CAVEAT`
- next_task: `TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger`
- runtime_observe_started: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Candidate Scoring

- `COMPOSITE_HIGH_INTENSITY_RULE`: `SELECTED` (density `0.06747`, score `0.9575`)
- `P95_RISK_CAP_SCORE`: `TOO_NARROW_MISSED_STRESS_RISK` (density `0.06747`, score `0.3725`)
- `P90_RISK_CAP_SCORE`: `TOO_BROAD_OVERBINDING_RISK` (density `0.06747`, score `0.3225`)

## Interpretation Boundary

Selected rule 只允许作为 TRADING-2336 observe-only event logger 的 research input；它不是 production rule，不是 automatic exposure cap，不是减仓建议，也不能生成 broker action。