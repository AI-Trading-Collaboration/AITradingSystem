# Dynamic strategy candidate budget and anti-overfit guardrails

## Budget

- max new candidate families：`6`
- max candidates per family：`3`
- max total new candidates for 2386：`12`
- selected new candidates：`12`
- within total budget：`true`

## Anti-overfit guardrails

- `pre_register_candidate_family_hypotheses_before_2386`
- `cap_new_candidates_at_12_for_default_2386_run`
- `compare_every_candidate_to_static_lower_turnover_and_ranking_top_references`
- `evaluate_cost_turnover_time_slice_and_regime_slice_before_owner_review`
- `forbid_post_hoc_metric_cherry_picking`
- `keep_monthly_rebalance_out_of_primary_decision_path`

## Forbidden paths

- `generate_unbounded_parameter_grid`
- `optimize_only_for_total_return`
- `use_monthly_rebalance_as_primary`
- `ignore_cost_and_slippage`
- `ignore_turnover`
- `ignore_time_slice_failures`
- `ignore_regime_slice_failures`
- `accept_candidate_without_static_baseline_comparison`
- `accept_candidate_without_lower_turnover_reference_comparison`
- `accept_candidate_without_ranking_top_reference_comparison`
