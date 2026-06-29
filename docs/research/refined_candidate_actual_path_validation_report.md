# Refined Candidate Actual-Path Validation Report

最后更新：2026-06-30

## Summary

- status: `REFINED_CANDIDATE_ACTUAL_PATH_EVIDENCE_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- candidate_count: `3`
- actual_path_record_count: `95220`
- validation_eligible_record_count: `73188`
- data_quality_status: `PASS_WITH_WARNINGS`
- guardrail_status_counts: `{'PASS_WITH_WARNINGS': 3}`
- comparison_label_counts: `{'REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED': 2, 'REFINED_WORSE': 1}`
- promotion_allowed: `false`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`

## Candidate Scorecards

|refined_candidate_id|eligible|alignment_rate|confidence_weighted|high_conviction_alignment|guardrail_status|recommended_status|
|---|---:|---:|---:|---:|---|---|
|`baseline_plus_trend_structure_refined_confidence_v1`|24108|0.39182|-0.108368|0.547548|`PASS_WITH_WARNINGS`|`REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH`|
|`risk_appetite_refined_confidence_v1`|24108|0.402729|0.16782|0.0|`PASS_WITH_WARNINGS`|`REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED`|
|`volatility_regime_refined_confidence_v1`|24972|0.329329|-0.138204|0.494896|`PASS_WITH_WARNINGS`|`REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH`|

TRADING-2289 只验证 refined actual-path evidence。high-confidence ratio 提升不等于策略有效，必须同时检查 high-conviction alignment、false risk-on / false risk-off cost 和 guardrails。

即使出现 owner review candidate recommendation，也只是进入 owner review，不是 promotion、paper-shadow、production 或 broker readiness。
