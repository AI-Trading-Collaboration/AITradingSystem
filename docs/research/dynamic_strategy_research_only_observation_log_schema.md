# 动态策略 research-only observation log schema

- schema version：`dynamic_strategy_research_only_observation_log.v1`
- schema mode：`RESEARCH_ONLY_SCHEMA_PLAN`

## identity

`observation_id`, `as_of`, `generated_by_task`, `source_artifact`, `candidate_id`, `candidate_version`, `execution_cadence`

## candidate_context

`primary_observation_candidate`, `ranking_top_from_2365`, `robustness_top_from_2366`, `gate_decision_from_2367`, `owner_decision_from_2371`

## signal_context

`signal_state`, `advisory_valid_from`, `advisory_valid_until`, `signal_horizon`, `valid_until_window_state`

## portfolio_preview

`reference_weight`, `proposed_research_weight`, `proposed_weight_delta`, `risk_cap_state`, `constraint_state`, `cooldown_state`, `no_trade_reason`

## cost_turnover

`expected_turnover`, `transaction_cost_bps`, `slippage_bps`, `estimated_cost_drag`, `turnover_cap_state`

## comparison

`static_baseline_comparison`, `ranking_top_candidate_comparison`, `robustness_top_candidate_comparison`, `dynamic_vs_static_preview_gap`

## review

`observation_decision`, `owner_review_required`, `review_reason`, `escalation_flag`

## guardrails

`research_only_observation`, `paper_shadow_enabled`, `paper_trade_created`, `shadow_position_created`, `event_append_enabled`, `outcome_binding_enabled`, `production_enabled`, `broker_action_enabled`, `daily_report_generated`
