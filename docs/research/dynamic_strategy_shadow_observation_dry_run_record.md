# 动态策略 shadow observation dry-run record

## identity

- `observation_id`：`TRADING-2369_2026-07-05_dynamic_regime_overlay_v0_4_lower_turnover`
- `as_of`：`2026-07-05`
- `source_task`：`TRADING-2369`
- `candidate_id`：`dynamic_regime_overlay_v0_4_lower_turnover`
- `candidate_version`：`source_artifact_candidate_from_trading_2367`
- `execution_cadence`：`valid_until_window`

## signal_state

- `signal_state`：`SOURCE_ARTIFACT_PREVIEW_ONLY_NOT_RECOMPUTED`
- `advisory_valid_from`：`2026-07-05`
- `advisory_valid_until`：`NOT_COMPUTED_NO_FRESH_MARKET_DATA`
- `signal_horizon`：`valid_until_window`
- `signal_confidence_if_available`：`None`

## portfolio_preview

- `reference_weight`：`None`
- `proposed_research_weight`：`None`
- `proposed_weight_delta`：`None`
- `max_single_step_weight_delta`：`None`
- `risk_cap_state`：`NOT_RECOMPUTED_PRIOR_ARTIFACT_DRY_RUN`
- `constraint_state`：`NO_SOURCE_CONSTRAINT_OR_STALE_SIGNAL_HITS_IN_PRIOR_ARTIFACT`
- `cooldown_state`：`NOT_SEVERE`
- `no_trade_reason`：`RESEARCH_ONLY_DRY_RUN_NO_EXECUTION`

## cost_and_turnover

- `expected_turnover`：`2.04`
- `transaction_cost_bps`：`10.2`
- `slippage_bps`：`10.2`
- `estimated_cost_drag`：`0.00204`
- `turnover_cap_state`：`OWNER_REVIEW_REQUIRED_TURNOVER_NOT_ACCEPTABLE_AFTER_2366`

## comparison

- `static_baseline_comparison`：`{'candidate_id': 'static_baseline', 'roles': ['static_baseline'], 'decision': 'STATIC_BASELINE_REFERENCE', 'cost_adjusted_return': 0.192557, 'dynamic_vs_static_gap': 0.0, 'max_drawdown': -0.140068, 'turnover': 0.0, 'turnover_acceptable_after_2366': None, 'cooldown_fragility': 'NOT_APPLICABLE_STATIC_BASELINE', 'review_priority_rank': 3.0}`
- `ranking_top_candidate_comparison`：`{'candidate_id': 'equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1', 'roles': ['ranking_top_from_2365'], 'decision': 'OWNER_REVIEW_REQUIRED', 'cost_adjusted_return': 0.213859, 'dynamic_vs_static_gap': 0.021302, 'max_drawdown': -0.183642, 'turnover': 1.964574, 'turnover_acceptable_after_2366': False, 'cooldown_fragility': 'NOT_SEVERE', 'review_priority_rank': 2.0}`
- `robustness_top_candidate_comparison`：`{'candidate_id': 'dynamic_regime_overlay_v0_4_lower_turnover', 'roles': ['robustness_top_from_2366', 'current_dynamic_default'], 'decision': 'OWNER_REVIEW_REQUIRED', 'cost_adjusted_return': 0.194762, 'dynamic_vs_static_gap': 0.002205, 'max_drawdown': -0.122866, 'turnover': 2.04, 'turnover_acceptable_after_2366': False, 'cooldown_fragility': 'NOT_SEVERE', 'review_priority_rank': 1.0}`
- `current_dynamic_default_if_available`：`{'candidate_id': 'dynamic_regime_overlay_v0_4_lower_turnover', 'roles': ['robustness_top_from_2366', 'current_dynamic_default'], 'decision': 'OWNER_REVIEW_REQUIRED', 'cost_adjusted_return': 0.194762, 'dynamic_vs_static_gap': 0.002205, 'max_drawdown': -0.122866, 'turnover': 2.04, 'turnover_acceptable_after_2366': False, 'cooldown_fragility': 'NOT_SEVERE', 'review_priority_rank': 1.0}`
- `dynamic_vs_static_preview_gap`：`0.002205`
- `comparison_candidates`：`['static_baseline', 'equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1', 'dynamic_regime_overlay_v0_4_lower_turnover']`

## review

- `owner_review_required`：`True`
- `review_reason`：`TRADING-2367 gate decision remains OWNER_REVIEW_REQUIRED; turnover requires owner review after TRADING-2366; ranking top and robustness top diverge`
- `escalation_flag`：`OWNER_REVIEW_REQUIRED`
- `observation_decision`：`OWNER_REVIEW_REQUIRED`

## guardrails

- `research_only_shadow_observation`：`True`
- `observation_mode`：`RESEARCH_ONLY_DRY_RUN`
- `paper_shadow_enabled`：`False`
- `paper_trade_created`：`False`
- `shadow_position_created`：`False`
- `event_append_enabled`：`False`
- `event_append_attempted`：`False`
- `outcome_binding_enabled`：`False`
- `outcome_binding_attempted`：`False`
- `scheduler_enabled`：`False`
- `scheduled_task_created`：`False`
- `production_enabled`：`False`
- `broker_action_enabled`：`False`
- `broker_action_attempted`：`False`
- `daily_report_generated`：`False`
