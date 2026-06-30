# Scope-Narrowed Candidate Actual-Path Validation Report

最后更新：2026-06-30

TRADING-2292 读取 TRADING-2291 scope-narrowed artifacts，只验证 `scope_active=true` active records；inactive records 仅作为 reference。

TRADING-2291 已生成 `baseline_plus_trend_structure_scope_narrowed_confirmation_v1` 和 `volatility_regime_scope_narrowed_risk_cap_v1`，并将 `risk_appetite_refined_confidence_v1` current form archive。

- status: `SCOPE_NARROWED_ACTUAL_PATH_VALIDATION_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- requested_date_range: `scope_narrowed_artifact_decision_timestamps`
- active_record_count_total: `4040`
- eligible_active_record_count_total: `3108`
- source_data_quality_status: `PASS_WITH_WARNINGS`
- risk_appetite_refined_confidence_v1: `current_form_archived`，不参与验证
- baseline_plus_trend_structure_scope_narrowed_confirmation_v1: `confirmation_only` validation
- volatility_regime_scope_narrowed_risk_cap_v1: `risk_cap_only` validation
- next_task_recommendation: `TRADING-2293_Scope_Narrowed_Forward_Observe_Readiness_Review`
- promotion_allowed: `false`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`

## Active vs Inactive

|candidate|usage|delta|label|
|---|---|---:|---|
|`baseline_plus_trend_structure_scope_narrowed_confirmation_v1`|`confirmation_only`|-0.010745|`ACTIVE_SCOPE_WORSE`|
|`volatility_regime_scope_narrowed_risk_cap_v1`|`risk_cap_only`|0.075184|`ACTIVE_SCOPE_OUTPERFORMS_REFERENCE`|

## State Recommendation

|candidate|status|forward_observe|
|---|---|---:|
|`baseline_plus_trend_structure_scope_narrowed_confirmation_v1`|`SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED`|False|
|`volatility_regime_scope_narrowed_risk_cap_v1`|`SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE`|True|

Forward observe candidate 仅为下一步 readiness review recommendation，不是 paper-shadow、production 或 broker readiness。
本报告不生成新的 candidate、不修改 TRADING-2291 artifacts、不改变 TRADING-2281 / 2285 / 2289 / 2291 既有结论。
