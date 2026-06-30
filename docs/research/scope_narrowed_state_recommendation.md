# Scope-Narrowed State Recommendation

最后更新：2026-06-30

- next_task_recommendation: `TRADING-2293_Scope_Narrowed_Forward_Observe_Readiness_Review`
- owner_review_candidate_count: `0`
- promotion_allowed: `false`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`

|candidate|status|forward_observe_candidate|sample|data_quality|
|---|---|---:|---|---|
|`baseline_plus_trend_structure_scope_narrowed_confirmation_v1`|`SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED`|False|`SAMPLE_SUFFICIENT`|`PASS_WITH_WARNINGS`|
|`volatility_regime_scope_narrowed_risk_cap_v1`|`SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE`|True|`SAMPLE_SUFFICIENT`|`PASS_WITH_WARNINGS`|

允许状态只限 scope-narrowed research recommendation。禁止输出 `PROMOTION_READY`、`PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 `BROKER_READY`。
