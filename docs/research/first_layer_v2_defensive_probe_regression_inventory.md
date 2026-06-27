# First-Layer V2 Defensive Probe Regression Inventory

- 状态：`FIRST_LAYER_V2_DEFENSIVE_PROBE_REGRESSION_INVENTORY_READY_PROMOTION_BLOCKED`
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
- probe_count_per_policy: `8`
- coverage_pass_policy_ids: `['wf_252d_initial', 'wf_expanding_initial']`
- coverage_pass_probe_row_count: `16`
- coverage_pass_regressed_probe_count: `5`
- coverage_pass_regressed_probe_ids: `['balanced_dynamic_probe', 'defensive_overlay_probe', 'drawdown_control_probe']`
- defensive_probe_regression_count: `5`
- return_seeking_improvement_count: `9`
- risk_on_diagnostic_improvement_count: `2`
- target_path_metrics_used_for_pass: `False`

## Coverage-Pass Probe Inventory

| policy_id | probe_id | role_group | improved_vs_flat | annual_delta | calmar_delta | regression_type |
|---|---|---|---:|---:|---:|---|
| wf_252d_initial | defensive_overlay_probe | defensive_overlay | False | -0.01401 | 0.257374 | ['defensive_probe_regression'] |
| wf_252d_initial | balanced_dynamic_probe | balanced_dynamic | False | -0.012391 | 0.214551 | ['defensive_probe_regression'] |
| wf_252d_initial | drawdown_control_probe | drawdown_control | False | -0.028335 | 0.027834 | ['defensive_probe_regression'] |
| wf_252d_initial | no_tqqq_return_seeking_probe | return_seeking | True | -0.013151 | 0.236765 | ['return_seeking_improvement'] |
| wf_252d_initial | low_tqqq_balanced_growth_probe | return_seeking | True | -0.011613 | 0.206351 | ['return_seeking_improvement'] |
| wf_252d_initial | qqq_heavy_growth_probe | return_seeking | True | -0.012133 | 0.204473 | ['return_seeking_improvement'] |
| wf_252d_initial | capped_risk_on_diagnostic_probe | risk_on_diagnostic | True | -0.010118 | 0.167393 | ['risk_on_diagnostic_improvement'] |
| wf_252d_initial | asymmetric_risk_on_slow_confirm_probe | return_seeking | True | -0.011613 | 0.206351 | ['return_seeking_improvement'] |
| wf_expanding_initial | defensive_overlay_probe | defensive_overlay | False | -0.012112 | 0.339695 | ['defensive_probe_regression'] |
| wf_expanding_initial | balanced_dynamic_probe | balanced_dynamic | True | -0.003391 | 0.564555 | ['return_seeking_improvement'] |
| wf_expanding_initial | drawdown_control_probe | drawdown_control | False | -0.01737 | 0.264806 | ['defensive_probe_regression'] |
| wf_expanding_initial | no_tqqq_return_seeking_probe | return_seeking | True | -0.007721 | 0.421721 | ['return_seeking_improvement'] |
| wf_expanding_initial | low_tqqq_balanced_growth_probe | return_seeking | True | -0.001296 | 0.483639 | ['return_seeking_improvement'] |
| wf_expanding_initial | qqq_heavy_growth_probe | return_seeking | True | -0.007548 | 0.295866 | ['return_seeking_improvement'] |
| wf_expanding_initial | capped_risk_on_diagnostic_probe | risk_on_diagnostic | True | 0.004895 | 0.406803 | ['risk_on_diagnostic_improvement'] |
| wf_expanding_initial | asymmetric_risk_on_slow_confirm_probe | return_seeking | True | -0.001296 | 0.483639 | ['return_seeking_improvement'] |
