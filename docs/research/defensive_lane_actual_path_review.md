# Defensive Lane Actual-Path Matrix

- 状态：`DEFENSIVE_LANE_ACTUAL_PATH_READY_PROMOTION_BLOCKED`
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
- model_id: `defensive_preservation_lane_v1`
- probe_count: `3`
- modeled_defensive_probe_count: `2`
- defensive_probe_no_regression: `True`
- drawdown_not_worse: `True`
- add_risk_used: `False`
- risk_on_used: `False`
- tqqq_signal_used: `False`
- limited_adjustment_reference_included: `True`
- promotion_status: `blocked`

## Actual Path Rows

| probe_id | annual_return | max_drawdown | delta_vs_neutral | no_regression |
|---|---:|---:|---:|---|
| defensive_overlay_probe | 0.118029 | -0.177727 | 0.040207 | True |
| drawdown_control_probe | 0.129728 | -0.17948 | 0.073022 | True |
| limited_adjustment_reference | 0.192658 | -0.116204 | None | None |
