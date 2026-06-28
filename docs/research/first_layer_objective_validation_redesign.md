# First-layer objective validation redesign

- status: `FIRST_LAYER_OBJECTIVE_VALIDATION_REDESIGN_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- requested_date_range: `2022-12-01` to `latest`
- actual_signal_range: `2023-02-22` to `2026-03-27`
- data_quality_status: `PASS_WITH_WARNINGS`
- safety: `validation_ready=false`, `promotion_allowed=false`, `paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`

## 结论

first-layer objective 已改写为可审计的 validation contract，但当前只能用于 TRADING-2273 challenger experiment 设计输入。2022 stress slice 没有 signal coverage，free / low-cost proxy 也没有替代 true breadth，因此不能宣称 validation-ready 或恢复任何 gate。

## Objective terms

|term_id|direction|current_baseline_value|validation_role|promotion_interpretation|
|---|---|---:|---|---|
|`false_risk_on_cost`|minimize|`198`|primary_downside_cost|diagnostic_contract_only_not_promotion|
|`false_risk_off_cost`|minimize|`499`|missed_upside_cost|diagnostic_contract_only_not_promotion|
|`drawdown_warning_lead_time`|maximize_lead_time_and_minimize_late_events|`5`|drawdown_warning_objective|diagnostic_contract_only_not_promotion|
|`recovery_delay_days`|minimize_delay_and_late_events|`5`|recovery_reentry_objective|diagnostic_contract_only_not_promotion|
|`regime_flip_penalty`|minimize|`1.956242`|stability_penalty|diagnostic_contract_only_not_promotion|
|`benchmark_consistency_score`|maximize|`0.392621`|cross_benchmark_consistency_check|diagnostic_contract_only_not_promotion|
|`stress_slice_minimum_requirements`|satisfy_all_required_slices|`3/4 slices covered`|stress_and_regime_coverage_gate|diagnostic_contract_only_not_promotion|

## Stress slice requirements

|slice|coverage_status|signal_obs|min_obs|requirement_met|
|---|---|---:|---:|---:|
|`2022_bear_rate_shock`|`NO_SIGNAL_COVERAGE`|0|60|False|
|`2023_recovery`|`SIGNAL_COVERED`|216|60|True|
|`2024_ai_concentration`|`SIGNAL_COVERED`|252|60|True|
|`2025_2026_trial_like_window`|`SIGNAL_COVERED`|309|60|True|

## Validation blockers

- stress_validation_allowed: `False`
- blocked_slice_ids: `2022_bear_rate_shock`
- true_breadth_replaced: `False`
- proxy_validation_blocker: `TRUE_BREADTH_NOT_REPLACED_BY_FREE_OR_LOW_COST_PROXY`
- blocking_conditions: `STRESS_SLICE_MINIMUM_REQUIREMENTS_NOT_MET,2022_STRESS_SLICE_NO_SIGNAL_COVERAGE,TRUE_BREADTH_NOT_REPLACED,OWNER_REVIEW_REQUIRED_BEFORE_CHALLENGER_PROMOTION`
