# Scope-Narrowed Candidate Regeneration Report

TRADING-2291 只生成 scope-narrowed candidate-bound artifacts，不执行 actual-path validation。

- status: `SCOPE_NARROWED_CANDIDATE_ARTIFACTS_READY_ACTUAL_PATH_VALIDATION_BLOCKED`
- included_candidate_count: `2`
- archived_candidate_count: `1`
- next_task: `TRADING-2292_Scope_Narrowed_Candidate_Actual_Path_Validation`

|scope_narrowed_candidate_id|refined_candidate_id|usage_role|active_record_count|inactive_record_count|
|---|---|---|---|---|
|baseline_plus_trend_structure_scope_narrowed_confirmation_v1|baseline_plus_trend_structure_refined_confidence_v1|confirmation_only|3667|27761|
|volatility_regime_scope_narrowed_risk_cap_v1|volatility_regime_refined_confidence_v1|risk_cap_only|373|31991|

所有输出固定 promotion / paper-shadow / production / broker action 为 false / none；
scope narrowing 不是 owner approval，也不是 paper-shadow、production 或 broker readiness。
