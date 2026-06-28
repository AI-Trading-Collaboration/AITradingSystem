# First-Layer Performance Gate Acceptance Audit

## 摘要

- task_id: `TRADING-2274_FIRST_LAYER_PERFORMANCE_GATE_ACCEPTANCE_AUDIT`; market_regime: `ai_after_chatgpt`; requested_start: `2022-12-01`; actual_signal_start: `2023-02-22`; data_quality_status: `PASS_WITH_WARNINGS`
- mandatory performance gate current accept count: `1` / `4`; active selection rule accept count remains `0`.
- offline validation-ready challenger rows: `4`; complete two-layer actual-path rows available: `0`.
- promotion_allowed=`false`, paper_shadow_allowed=`false`, production_allowed=`false`, broker_action=`none`.
- best_actual_path_candidate: `wf_504d_baseline` = `0.070283`

## Gate 结论

| gate | before | after_current | gate_marginal_utility | opportunity_cost | threshold_stability | recommended_action |
|---|---:|---:|---|---:|---|---|
|`actual_path_improved_probe_count_min`|`1`|`1`|`neutral`|`0.0`|`stable`|`convert_to_score_penalty`|
|`no_major_regression_in_defensive_probe`|`3`|`1`|`positive`|`0.000404`|`sensitive_to_threshold`|`keep_as_hard_gate`|
|`2022_slice_not_worse_than_flat_reference`|`1`|`1`|`neutral`|`0.0`|`sensitive_to_threshold`|`keep_as_performance_gate`|
|`net_of_cost_not_worse`|`1`|`1`|`neutral`|`0.0`|`stable`|`convert_to_score_penalty`|
|`not_2023_plus_only`|`2`|`1`|`negative`|`0.070283`|`unstable_material_opportunity_cost`|`remove_gate`|
|`not_beta_dependency`|`1`|`1`|`inconclusive`|`0.0`|`insufficient_candidate_level_evidence`|`convert_to_owner_review`|
|`not_tqqq_dependency`|`1`|`1`|`inconclusive`|`0.0`|`insufficient_candidate_level_evidence`|`convert_to_owner_review`|
|`probability_threshold_0_55`|`1`|`1`|`inconclusive`|`0.0`|`insufficient_candidate_level_evidence`|`convert_to_score_penalty`|
|`probability_threshold_0_60`|`1`|`1`|`inconclusive`|`0.0`|`insufficient_candidate_level_evidence`|`convert_to_score_penalty`|
|`all_slices_not_worse`|`1`|`1`|`inconclusive`|`0.0`|`incomplete_all_slice_actual_path_evidence`|`convert_to_owner_review`|
|`no_slice_regression`|`1`|`1`|`neutral`|`0.0`|`sensitive_to_threshold`|`keep_as_performance_gate`|

## Rejected Candidate Counterfactual

| policy_id | failed_current_gates | utility_proxy | owner_review_candidate |
|---|---|---:|---|
|`wf_504d_baseline`|`not_2023_plus_only`|`0.070283`|`True`|
|`wf_expanding_initial`|`no_major_regression_in_defensive_probe`|`0.000404`|`False`|
|`wf_252d_initial`|`no_major_regression_in_defensive_probe`|`-0.014885`|`False`|

## Candidate Utility Proxy

| policy_id | avg_return | avg_abs_drawdown | avg_sharpe | avg_calmar | avg_turnover | utility_proxy |
|---|---:|---:|---:|---:|---:|---:|
|`wf_504d_baseline`|`0.159078`|`0.093198`|`1.457731`|`1.72452`|`75.587344`|`0.070283`|
|`wf_378d_initial`|`0.124552`|`0.086262`|`1.211746`|`1.453457`|`73.268448`|`0.041538`|
|`wf_expanding_initial`|`0.104402`|`0.109231`|`0.927045`|`0.96675`|`88.299723`|`0.000404`|
|`wf_252d_initial`|`0.095961`|`0.129256`|`0.869776`|`0.749296`|`55.614916`|`-0.014885`|

## Challenger Rows

| experiment_id | validation_ready | actual_path_status |
|---|---|---|
|`baseline`|`True`|`unavailable_actual_path_not_run`|
|`baseline_plus_trend_structure`|`True`|`unavailable_actual_path_not_run`|
|`volatility_regime`|`True`|`unavailable_actual_path_not_run`|
|`risk_appetite`|`True`|`unavailable_actual_path_not_run`|
|`equal_cap_weight_divergence`|`False`|`unavailable_actual_path_not_run`|
|`combined_proxy`|`False`|`unavailable_actual_path_not_run`|

## Evidence Limitations

- `challenger_actual_path_not_run`: offline validation-ready challenger rows cannot be scored as complete two-layer actual-path candidates action=`run_future_candidate_level_actual_path_backtest`
- `candidate_level_objective_deltas_unavailable`: false risk-on/off, lead-time, recovery-delay and benchmark consistency deltas are not present in frozen actual-path policy rows action=`extend_actual_path_backtest_to_objective_terms`
- `candidate_level_dependency_deltas_unavailable`: beta/TQQQ dependency evidence is lane-level diagnostic evidence, not row-level gate proof for each first-layer coverage policy action=`convert_dependency_gates_to_owner_review_until_row_level_evidence_exists`
- `candidate_level_probability_distribution_unavailable`: 0.55/0.60 probability thresholds cannot be calibrated from the frozen actual-path matrix action=`treat_probability_thresholds_as_score_penalty_or_owner_review`
- `all_required_slice_actual_path_incomplete`: all-slices gate cannot be promoted to a hard performance gate because required stress slice evidence remains incomplete action=`owner_review_or_future_all_slice_backtest`
- `no_challenger_counterfactual_actual_path_rows`: challenger matrix contributes readiness context only action=`do_not_use_challenger_rows_for_promotion`
- `return_seeking_2023_plus_dependency_diagnostic`: 2023+ dependency exists as diagnostic evidence but not as candidate-level gate delta action=`owner_review_before_hard_gate_application`

## 产物

- `gate_ablation_matrix`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\gate_ablation_matrix.json`
- `threshold_sensitivity_report`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\threshold_sensitivity_report.json`
- `rejected_candidate_counterfactual_report`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\rejected_candidate_counterfactual_report.json`
- `recommended_gate_policy`: `D:\Work\AITradingSystem\outputs\research_trends\first_layer_performance_gate_audit\recommended_gate_policy.yaml`
- `gate_acceptance_audit_report`: `D:\Work\AITradingSystem\docs\research\gate_acceptance_audit_report.md`

## Safety Boundary

本 audit 只读既有 current-state / objective spec / challenger matrix / frozen actual-path evidence，不改变 active selection rule。PIT、no-lookahead、data quality、actual-path、owner approval 和 production boundary 仍是 hard gates，不参与收益表现豁免。
