# Refined Candidate State Recommendation

最后更新：2026-06-30

|refined_candidate_id|status|owner_review_candidate|guardrail|comparison|
|---|---|---:|---|---|
|`baseline_plus_trend_structure_refined_confidence_v1`|`REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH`|False|`PASS_WITH_WARNINGS`|`REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED`|
|`risk_appetite_refined_confidence_v1`|`REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED`|False|`PASS_WITH_WARNINGS`|`REFINED_WORSE`|
|`volatility_regime_refined_confidence_v1`|`REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH`|False|`PASS_WITH_WARNINGS`|`REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED`|

允许状态只限 refined research recommendation。禁止输出 `PROMOTION_READY`、`PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 `BROKER_READY`。
