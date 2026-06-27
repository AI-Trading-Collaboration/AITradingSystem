# First-Layer V2 Policy Variant Stability Review

- 状态：`FIRST_LAYER_V2_POLICY_VARIANT_STABILITY_READY_PROMOTION_BLOCKED`
- 市场阶段：`ai_after_chatgpt`
- promotion_allowed：`False`
- paper_shadow_allowed：`False`
- production_allowed：`False`
- broker_action：`none`

## 摘要
- research_window_id: `exact_three_asset_validated`
- requested_start: `2021-02-22`
- actual_start: `2021-02-22`
- actual_portfolio_start: `2021-02-22`
- end: `latest`
- window_role: `primary_validated`
- data_quality_contract: `secondary_cross_checked`
- exact_or_proxy: `exact`
- policy_count: `5`
- coverage_pass_policy_ids: `['wf_252d_initial', 'wf_expanding_initial']`
- late_window_improved_policy_ids: `['wf_504d_baseline', 'wf_378d_initial']`
- coverage_pass_average_risk_off_recall: `0.292042`
- shorter_train_window_instability_detected: `True`
- expanding_mitigates_but_does_not_remove_defensive_regression: `True`
- why_504d_378d_still_8_of_8_improved: `their effective windows start after, or too late within, 2022 stress and are not eligible for coverage-aware owner review`
- target_path_metrics_used_for_pass: `False`

## Policy Stability

| policy_id | first_prediction | train_min | do_not_de_risk_fp | add_risk_fp | diagnosis |
|---|---|---:|---:|---:|---|
| wf_504d_baseline | 2023-02-22 | 504 | 724 | 611 | LATE_WINDOW_8_OF_8_NOT_COVERAGE_ELIGIBLE |
| wf_378d_initial | 2022-08-22 | 378 | 692 | 223 | LATE_WINDOW_8_OF_8_NOT_COVERAGE_ELIGIBLE |
| wf_252d_initial | 2022-02-18 | 252 | 372 | 90 | COVERAGE_PASS_SHORT_TRAIN_DEFENSIVE_REGRESSION |
| wf_expanding_initial | 2022-02-18 | 252 | 871 | 657 | EXPANDING_MITIGATES_RETURN_SEEKING_BUT_DEFENSIVE_REGRESSION_REMAINS |
| wf_warm_start_diagnostic | 2021-02-22 | 0 | 0 | 0 | DIAGNOSTIC_ONLY_NOT_OWNER_REVIEWABLE |
