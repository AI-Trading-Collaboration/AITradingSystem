# First-Layer V2 2022 Defensive Regression Slice Review

- 状态：`FIRST_LAYER_V2_2022_DEFENSIVE_REGRESSION_SLICE_READY_PROMOTION_BLOCKED`
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
- data_quality_status: `PASS_WITH_WARNINGS`
- prior_slice_status: `FIRST_LAYER_V2_2022_STRESS_RECOVERY_SLICE_READY_PROMOTION_BLOCKED`
- slice_count: `3`
- policy_count: `5`
- coverage_pass_policy_ids: `['wf_252d_initial', 'wf_expanding_initial']`
- coverage_pass_average_false_re_risk_cost: `0.000277`
- coverage_pass_average_false_add_risk_cost: `0.000237`
- coverage_pass_average_avoided_drawdown: `0.023557`
- 2022_regression_conclusion: `DEFENSIVE_REGRESSION_PRESENT_IN_2022_AND_TRANSITION_SLICE`
- target_path_metrics_used_for_pass: `False`

## 2022 Slices

| policy_id | slice_id | predictions | state_distribution | qqq_equiv | false_re_risk | avoided_drawdown |
|---|---|---:|---|---:|---:|---:|
| wf_252d_initial | 2022_drawdown_slice | 210 | {'risk_off': 172, 'defensive': 36, 'neutral': 2} | 0.30994 | 0.0 | 0.036839 |
| wf_252d_initial | 2022_recovery_slice | 255 | {'defensive': 241, 'risk_off': 14} | 0.47549 | 0.0 | 0.015274 |
| wf_252d_initial | post_chatgpt_transition_slice | 228 | {'defensive': 228} | 0.4875 | 0.0 | 0.009722 |
| wf_expanding_initial | 2022_drawdown_slice | 210 | {'risk_off': 206, 'neutral': 2, 'defensive': 2} | 0.274524 | 0.0 | 0.039615 |
| wf_expanding_initial | 2022_recovery_slice | 255 | {'risk_off': 194, 'neutral': 48, 'defensive': 13} | 0.352843 | 0.0 | 0.02491 |
| wf_expanding_initial | post_chatgpt_transition_slice | 228 | {'risk_off': 154, 'neutral': 44, 'risk_on': 14, 'defensive': 12, 'constructive': 4} | 0.41284 | 0.001661 | 0.014984 |
