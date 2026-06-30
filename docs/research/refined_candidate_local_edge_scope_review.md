# Refined Candidate Local Edge Scope Review

最后更新：2026-06-30

TRADING-2289 已完成 refined actual-path validation；owner review candidate recommendation 全部为 false。TRADING-2290 不生成 owner review package，只做 local edge / scope narrowing review。

## Summary

- status: `LOCAL_EDGE_SCOPE_REVIEW_READY_PROMOTION_BLOCKED`
- input_actual_path_records_from_2289: `95220`
- input_eligible_records_from_2289: `73188`
- owner_review_candidate_count: `0`
- scope_narrowing_candidate_count: `2`
- next_task_recommendation: `['TRADING-2291_Archive_Rejected_Candidate_Current_Form', 'TRADING-2291_Scope_Narrowed_Candidate_Regeneration']`

## Candidate Local Edge

|refined_candidate_id|status_from_2289|alignment|high_conviction_alignment|guardrail|local_edge_label|primary_scope_opportunity|
|---|---:|---:|---:|---|---|---|
|baseline_plus_trend_structure_refined_confidence_v1|REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH|0.39182|0.547548|PASS_WITH_WARNINGS|LOCAL_EDGE_PRESENT|trend_confirmation_high_conviction|
|risk_appetite_refined_confidence_v1|REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED|0.402729|0.0|PASS_WITH_WARNINGS|LOCAL_EDGE_NOT_FOUND|current_form_reject|
|volatility_regime_refined_confidence_v1|REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH|0.329329|0.494896|PASS_WITH_WARNINGS|LOCAL_EDGE_WEAK|risk_cap_or_veto_high_conviction|

本报告中的 local edge 只表示后续 scope-narrowed research 线索，不代表 promotion、paper-shadow、production 或 broker readiness。
