# First-Layer Performance Gate Acceptance Audit

## 摘要

- market_regime: `ai_after_chatgpt`; anchor_date: `2022-11-30`; requested_start: `2021-02-22`; actual_start: `2021-02-22`
- current_gate_accept_count: `0`; promotion/paper-shadow/production/broker 均 blocked/none。
- best_actual_path_candidate: `wf_504d_baseline` = `0.070283`

## Gate 结论

| gate | gate_marginal_utility | gate_failure_mode_reduced | opportunity_cost | recommended_action |
|---|---|---|---:|---|
|`coverage_pass_rule`|`negative`|`late_window_only_or_2023_plus_overfit`|`0.070283`|`convert_to_owner_review_evidence_gate`|
|`actual_path_improved_probe_count_min`|`neutral`|`weak_or_non_general_actual_path_improvement`|`0.0`|`mark_redundant_monitor_only`|
|`no_major_regression_in_defensive_probe`|`positive`|`defensive_or_drawdown_control_regression`|`0.000404`|`keep_current_gate`|
|`net_of_cost_not_worse`|`neutral`|`turnover_or_cost_drag_hidden_by_gross_return`|`0.0`|`retain_as_owner_review_audit_gate_pending_more_variation`|
|`2022_slice_not_worse_than_flat_reference`|`neutral`|`stress_recovery_slice_regression`|`0.0`|`retain_as_owner_review_audit_gate_pending_more_variation`|
|`same_risk_comparison_reported`|`neutral`|`same_risk_frontier_blind_spot`|`0.0`|`retain_as_audit_completeness_gate_not_performance_gate`|

## Rejected Candidate Counterfactual

| policy_id | failed_current_gates | utility_proxy | owner_review_candidate |
|---|---|---:|---|
|`wf_504d_baseline`|`coverage_pass_rule`|`0.070283`|`True`|
|`wf_378d_initial`|`coverage_pass_rule`|`0.041538`|`True`|
|`wf_expanding_initial`|`no_major_regression_in_defensive_probe`|`0.000404`|`False`|
|`wf_252d_initial`|`no_major_regression_in_defensive_probe`|`-0.014885`|`False`|

## Candidate Utility Proxy

| policy_id | avg_return | avg_abs_drawdown | avg_turnover | utility_proxy |
|---|---:|---:|---:|---:|
|`wf_504d_baseline`|`0.159078`|`0.093198`|`75.587344`|`0.070283`|
|`wf_378d_initial`|`0.124552`|`0.086262`|`73.268448`|`0.041538`|
|`wf_expanding_initial`|`0.104402`|`0.109231`|`88.299723`|`0.000404`|
|`wf_252d_initial`|`0.095961`|`0.129256`|`55.614916`|`-0.014885`|

## 产物

- `gate_ablation_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\gate_ablation_matrix.json`
- `threshold_sensitivity_report`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\threshold_sensitivity_report.json`
- `rejected_candidate_counterfactual_report`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\rejected_candidate_counterfactual_report.json`
- `recommended_gate_policy`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\recommended_gate_policy.yaml`
- `gate_acceptance_audit_report`: `D:\Work\AITradingSystem\docs\research\gate_acceptance_audit_report.md`

## Safety Boundary

本 audit 只读既有 actual-path / coverage / 2022 slice / defensive regression evidence，不改变 active selection rule。
