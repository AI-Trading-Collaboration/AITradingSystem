# 动态策略 shadow observation field schema

## identity

- `observation_id`
- `as_of`
- `source_task`
- `candidate_id`
- `candidate_version`
- `execution_cadence`

## signal_state

- `signal_state`
- `advisory_valid_from`
- `advisory_valid_until`
- `signal_horizon`
- `signal_confidence_if_available`

## portfolio_preview

- `current_reference_weight`
- `proposed_research_weight`
- `proposed_weight_delta`
- `max_single_step_weight_delta`
- `risk_cap_state`
- `constraint_state`
- `cooldown_state`

## cost_and_turnover

- `expected_turnover`
- `transaction_cost_bps`
- `slippage_bps`
- `estimated_cost_drag`
- `turnover_cap_state`

## comparison

- `static_baseline_weight`
- `static_baseline_expected_return_if_available`
- `ranking_top_candidate_weight`
- `robustness_top_candidate_weight`
- `dynamic_vs_static_preview_gap`

## guardrails

- `research_only_shadow_observation`
- `paper_shadow_enabled`
- `event_append_enabled`
- `outcome_binding_enabled`
- `production_enabled`
- `broker_action_enabled`

## review

- `owner_review_required`
- `review_reason`
- `escalation_flag`
- `observation_decision`

## Guardrail

- field schema 只供 future research-only dry-run 使用。
- schema 不允许 event append、outcome binding、paper trade、production 或 broker action。