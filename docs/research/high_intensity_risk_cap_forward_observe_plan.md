# High-Intensity Risk-Cap Forward Observe Plan

TRADING-2334 承接 TRADING-2333 `HIGH_INTENSITY_ONLY_FORWARD_OBSERVE` route，把 broad exposure-cap mechanics 收窄为 research-only high-intensity risk warning plan。本任务不启动 runtime observe，不执行新的 dry-run，不生成交易指令。

- status: `HIGH_INTENSITY_FORWARD_OBSERVE_PLAN_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- data_quality_status: `PASS_WITH_WARNINGS`
- data_validation_policy: `NOT_APPLICABLE_PRIOR_VALIDATED_2332_2333_ARTIFACTS_ONLY`
- prior_2333_overall_recommendation: `HIGH_INTENSITY_ONLY_FORWARD_OBSERVE`
- cap_binding_rate: `0.455422`
- return_proxy_delta: `-0.187258`
- drawdown_proxy_delta: `0.045294`
- readiness_status: `THRESHOLD_SELECTION_REQUIRED`
- next_task: `TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection`
- runtime_observe_started: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Threshold Candidates

- `P90_RISK_CAP_SCORE`: `TOO_BROAD_OVERBINDING_RISK` (density `0.06747`)
- `P95_RISK_CAP_SCORE`: `TOO_NARROW_MISSED_STRESS_RISK` (density `0.06747`)
- `COMPOSITE_HIGH_INTENSITY_RULE`: `CANDIDATE_FOR_2335_SELECTION` (density `0.06747`)

## 解释边界

当前 broad exposure-cap mechanics 不应继续作为 automatic exposure limiter。High-intensity trigger 仅作为 future forward observe / manual review context，medium / low intensity trigger 只记录，不触发观察升级。任何 actual-path outcome 必须在事件发生后自然填充，不得反向修改 event trigger。
