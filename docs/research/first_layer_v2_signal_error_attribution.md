# First-Layer V2 Signal Error Attribution

- 状态：`FIRST_LAYER_V2_SIGNAL_ERROR_ATTRIBUTION_READY_PROMOTION_BLOCKED`
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
- regressed_probe_ids: `['balanced_dynamic_probe', 'defensive_overlay_probe', 'drawdown_control_probe']`
- episode_row_count: `54`
- episode_selection_policy: `top_cost_events_per_regressed_probe_policy; reporting cap does not affect diagnosis`
- episode_report_limit_per_probe_policy: `16`
- forward_return_horizon_days: `20`
- primary_signal_error_diagnosis: `DEFENSIVE_REGRESSION_DUE_TO_FALSE_ADD_RISK`
- error_type_counts: `{'ADD_RISK_FALSE_POSITIVE': 42, 'DO_NOT_DE_RISK_FALSE_POSITIVE': 279, 'HIGH_CONFIDENCE_RISK_ON_FALSE_POSITIVE': 21}`
- feature_coverage_gap_detected: `False`
- train_window_instability_detected: `True`
- target_path_metrics_used_for_pass: `False`

## Error Type Counts

- ADD_RISK_FALSE_POSITIVE: `42`
- DO_NOT_DE_RISK_FALSE_POSITIVE: `279`
- HIGH_CONFIDENCE_RISK_ON_FALSE_POSITIVE: `21`

## Top Episodes
- `2022-04-04` `wf_252d_initial` `drawdown_control_probe` `DO_NOT_DE_RISK_FALSE_POSITIVE` cost=`0.21247` drawdown=`-0.151765`
- `2022-04-04` `wf_expanding_initial` `drawdown_control_probe` `DO_NOT_DE_RISK_FALSE_POSITIVE` cost=`0.21247` drawdown=`-0.151765`
- `2023-02-13` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.126327` drawdown=`-0.066488`
- `2023-02-14` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.126327` drawdown=`-0.066488`
- `2023-02-15` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.126327` drawdown=`-0.066488`
- `2023-01-31` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.125083` drawdown=`-0.065833`
- `2023-02-01` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.125083` drawdown=`-0.065833`
- `2023-01-23` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.124811` drawdown=`-0.056732`
- `2023-01-26` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.121102` drawdown=`-0.063738`
- `2023-01-27` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.121102` drawdown=`-0.063738`
- `2023-02-03` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.114563` drawdown=`-0.060296`
- `2023-02-08` `wf_expanding_initial` `drawdown_control_probe` `ADD_RISK_FALSE_POSITIVE` cost=`0.110082` drawdown=`-0.057938`
