# Return-Seeking Beta and TQQQ Attribution

- 状态：`RETURN_SEEKING_BETA_TQQQ_ATTRIBUTION_DEPENDENT_PROMOTION_BLOCKED`
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
- probe_count: `7`
- qqq_beta_dependency_suspected_count: `0`
- tqqq_beta_dependency_suspected_count: `5`
- no_tqqq_reference_avg_qqq_equivalent_exposure: `0.64068`
- promotion_status: `blocked`

## Attribution Rows

| probe_id | avg_qqq_equiv | tqqq_share | qqq_beta | tqqq_beta |
|---|---:|---:|---|---|
| balanced_dynamic_probe | 0.55234 | 0.175382 | False | True |
| drawdown_control_probe | 0.531338 | 0.211634 | False | True |
| no_tqqq_return_seeking_probe | 0.553175 | 0.0 | False | False |
| low_tqqq_balanced_growth_probe | 0.617755 | 0.156811 | False | True |
| qqq_heavy_growth_probe | 0.64068 | 0.0 | False | False |
| capped_risk_on_diagnostic_probe | 0.682336 | 0.283939 | False | True |
| asymmetric_risk_on_slow_confirm_probe | 0.617755 | 0.156811 | False | True |
