# First-Layer V2 Probe Role Group Comparison

- 状态：`FIRST_LAYER_V2_PROBE_ROLE_GROUP_COMPARISON_READY_PROMOTION_BLOCKED`
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
- coverage_pass_policy_ids: `['wf_252d_initial', 'wf_expanding_initial']`
- defensive_or_drawdown_regressed_count: `5`
- pure_return_seeking_probe_count: `8`
- pure_return_seeking_improved_count: `8`
- risk_on_diagnostic_probe_count: `2`
- risk_on_diagnostic_improved_count: `2`
- regression_concentration: `DEFENSIVE_AND_DRAWDOWN_CONTROL_WITH_BALANCED_EXCEPTION`
- return_seeking_value_supported: `True`
- risk_on_diagnostic_value_supported: `True`
- role_group_conclusion: `RETURN_SEEKING_DIAGNOSTIC_LAYER_POSSIBLE_DEFENSIVE_BLOCKED`

## Role Groups

| policy_id | role_group | improved | regressed | conclusion |
|---|---|---:|---:|---|
| wf_252d_initial | defensive_overlay | 0 | 1 | DEFENSIVE_OR_DRAWDOWN_REGRESSION |
| wf_252d_initial | drawdown_control | 0 | 1 | DEFENSIVE_OR_DRAWDOWN_REGRESSION |
| wf_252d_initial | balanced_dynamic | 0 | 1 | DEFENSIVE_OR_DRAWDOWN_REGRESSION |
| wf_252d_initial | return_seeking | 4 | 0 | RETURN_SEEKING_IMPROVES |
| wf_252d_initial | risk_on_diagnostic | 1 | 0 | RISK_ON_DIAGNOSTIC_IMPROVES |
| wf_expanding_initial | defensive_overlay | 0 | 1 | DEFENSIVE_OR_DRAWDOWN_REGRESSION |
| wf_expanding_initial | drawdown_control | 0 | 1 | DEFENSIVE_OR_DRAWDOWN_REGRESSION |
| wf_expanding_initial | balanced_dynamic | 1 | 0 | RETURN_SEEKING_IMPROVES |
| wf_expanding_initial | return_seeking | 4 | 0 | RETURN_SEEKING_IMPROVES |
| wf_expanding_initial | risk_on_diagnostic | 1 | 0 | RISK_ON_DIAGNOSTIC_IMPROVES |
