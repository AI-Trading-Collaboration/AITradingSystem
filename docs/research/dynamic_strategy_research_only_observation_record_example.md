# 动态策略 research-only observation record example

## identity

- `observation_id`：`TRADING-2373_2026-07-05_dynamic_regime_overlay_v0_4_lower_turnover`
- `as_of`：`2026-07-05`
- `generated_by_task`：`TRADING-2373`
- `source_artifact`：`D:\Work\AITradingSystem\outputs\research_strategies\dynamic_strategy_research_only_shadow_observation_dry_run\observation_dry_run_record.json`
- `candidate_id`：`dynamic_regime_overlay_v0_4_lower_turnover`
- `candidate_version`：`source_artifact_candidate_from_trading_2367`
- `execution_cadence`：`valid_until_window`

## candidate_context

- `primary_observation_candidate`：`dynamic_regime_overlay_v0_4_lower_turnover`
- `ranking_top_from_2365`：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- `robustness_top_from_2366`：`dynamic_regime_overlay_v0_4_lower_turnover`
- `gate_decision_from_2367`：`OWNER_REVIEW_REQUIRED`
- `owner_decision_from_2371`：`APPROVE_RESEARCH_ONLY_OBSERVATION_CONTINUE_WITH_NO_EXECUTION`

## signal_context

- `signal_state`：`SOURCE_ARTIFACT_PREVIEW_ONLY_NOT_RECOMPUTED`
- `advisory_valid_from`：`2026-07-05`
- `advisory_valid_until`：`NOT_COMPUTED_NO_FRESH_MARKET_DATA`
- `signal_horizon`：`valid_until_window`
- `valid_until_window_state`：`NOT_COMPUTED_NO_FRESH_MARKET_DATA`

## portfolio_preview

- `reference_weight`：`None`
- `proposed_research_weight`：`None`
- `proposed_weight_delta`：`None`
- `risk_cap_state`：`NOT_RECOMPUTED_PRIOR_ARTIFACT_DRY_RUN`
- `constraint_state`：`NO_SOURCE_CONSTRAINT_OR_STALE_SIGNAL_HITS_IN_PRIOR_ARTIFACT`
- `cooldown_state`：`NOT_SEVERE`
- `no_trade_reason`：`RESEARCH_ONLY_DRY_RUN_NO_EXECUTION`

## cost_turnover

- `expected_turnover`：`2.04`
- `transaction_cost_bps`：`10.2`
- `slippage_bps`：`10.2`
- `estimated_cost_drag`：`0.00204`
- `turnover_cap_state`：`OWNER_REVIEW_REQUIRED_TURNOVER_NOT_ACCEPTABLE_AFTER_2366`

## comparison

- `static_baseline_comparison`：`{'candidate_id': 'static_baseline', 'roles': ['static_baseline'], 'decision': 'STATIC_BASELINE_REFERENCE', 'cost_adjusted_return': 0.192557, 'dynamic_vs_static_gap': 0.0, 'max_drawdown': -0.140068, 'turnover': 0.0, 'turnover_acceptable_after_2366': None, 'cooldown_fragility': 'NOT_APPLICABLE_STATIC_BASELINE', 'review_priority_rank': 3.0}`
- `ranking_top_candidate_comparison`：`{'candidate_id': 'equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1', 'roles': ['ranking_top_from_2365'], 'decision': 'OWNER_REVIEW_REQUIRED', 'cost_adjusted_return': 0.213859, 'dynamic_vs_static_gap': 0.021302, 'max_drawdown': -0.183642, 'turnover': 1.964574, 'turnover_acceptable_after_2366': False, 'cooldown_fragility': 'NOT_SEVERE', 'review_priority_rank': 2.0}`
- `robustness_top_candidate_comparison`：`{'candidate_id': 'dynamic_regime_overlay_v0_4_lower_turnover', 'roles': ['robustness_top_from_2366', 'current_dynamic_default'], 'decision': 'OWNER_REVIEW_REQUIRED', 'cost_adjusted_return': 0.194762, 'dynamic_vs_static_gap': 0.002205, 'max_drawdown': -0.122866, 'turnover': 2.04, 'turnover_acceptable_after_2366': False, 'cooldown_fragility': 'NOT_SEVERE', 'review_priority_rank': 1.0}`
- `dynamic_vs_static_preview_gap`：`0.002205`

## review

- `observation_decision`：`OWNER_REVIEW_REQUIRED`
- `owner_review_required`：`True`
- `review_reason`：`TRADING-2367 gate decision remains OWNER_REVIEW_REQUIRED; turnover requires owner review after TRADING-2366; ranking top and robustness top diverge`
- `escalation_flag`：`OWNER_REVIEW_REQUIRED`

## guardrails

- `research_only_observation`：`True`
- `paper_shadow_enabled`：`False`
- `paper_trade_created`：`False`
- `shadow_position_created`：`False`
- `event_append_enabled`：`False`
- `outcome_binding_enabled`：`False`
- `production_enabled`：`False`
- `broker_action_enabled`：`False`
- `daily_report_generated`：`False`
