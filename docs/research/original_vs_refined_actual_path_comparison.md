# Original vs Refined Actual-Path Comparison

最后更新：2026-06-30

|original_candidate_id|refined_candidate_id|alignment_delta|confidence_weighted_delta|false_on_delta|false_off_delta|guardrail|label|
|---|---|---:|---:|---:|---:|---|---|
|`baseline_plus_trend_structure`|`baseline_plus_trend_structure_refined_confidence_v1`|-0.015265|-0.005892|-20.728394|-8.0105|`PASS_WITH_WARNINGS`|`REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED`|
|`risk_appetite`|`risk_appetite_refined_confidence_v1`|-0.015638|0.005924|-19.501802|-11.785804|`PASS_WITH_WARNINGS`|`REFINED_WORSE`|
|`volatility_regime`|`volatility_regime_refined_confidence_v1`|-0.006447|0.01731|0.0|-7.505309|`PASS_WITH_WARNINGS`|`REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED`|

Comparison 只回答 actual-path evidence 是否改善；不改变 TRADING-2285 original inconclusive 结论，也不产生 promotion readiness。
