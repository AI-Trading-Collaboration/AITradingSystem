# Return-Seeking Actual-Path Review

- 状态：`RETURN_SEEKING_ACTUAL_PATH_UPSIDE_WITH_DRAWDOWN_REGRESSION_PROMOTION_BLOCKED`
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
- model_id: `first_layer_composer_v2`
- reference_model_id: `no_return_seeking_reference`
- probe_count: `7`
- diagnostic_value_probe_count: `0`
- positive_return_delta_probe_count: `7`
- drawdown_regression_probe_count: `7`
- defensive_overlay_usage_allowed: `False`
- full_allocation_usage_allowed: `False`
- gated_integration_allowed: `False`
- promotion_status: `blocked`

## Actual Path Rows

| probe_id | return_delta | drawdown_delta | diagnostic_value |
|---|---:|---:|---|
| balanced_dynamic_probe | 0.025752 | -0.020726 | False |
| drawdown_control_probe | 0.036423 | -0.039603 | False |
| no_tqqq_return_seeking_probe | 0.01613 | -0.005482 | False |
| low_tqqq_balanced_growth_probe | 0.028748 | -0.017593 | False |
| qqq_heavy_growth_probe | 0.013569 | -0.005357 | False |
| capped_risk_on_diagnostic_probe | 0.041185 | -0.030645 | False |
| asymmetric_risk_on_slow_confirm_probe | 0.028748 | -0.017593 | False |
