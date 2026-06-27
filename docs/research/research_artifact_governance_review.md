# Research Artifact Governance Review

- 状态：`RESEARCH_ARTIFACT_GOVERNANCE_REVIEW_READY`
- market_regime：`ai_after_chatgpt`
- date_range：`2022-12-01` to `2026-06-26`
- data_quality_status：`PASS_WITH_WARNINGS`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Review Table

|artifact_id|schema_version|promotion_status|metric_namespace|failed_checks|governance_status|
|---|---|---|---|---|---|
|actual_path_edge_attribution_matrix|actual_path_edge_attribution_matrix.v1|BLOCKED|actual_path_only||PASS|
|dynamic_strategy_objective_gate_matrix|dynamic_strategy_objective_gate_matrix.v1|BLOCKED|actual_path_only||PASS|
|pit_data_availability_inventory|pit_data_availability_inventory.v1|BLOCKED|actual_path_only||PASS|
|dynamic_strategy_walk_forward_matrix|dynamic_strategy_walk_forward_matrix.v1|BLOCKED|actual_path_only||PASS|
|event_override_ex_ante_taxonomy|event_override_ex_ante_taxonomy.v1|BLOCKED|actual_path_only||PASS|
|risk_timing_quality_matrix|risk_timing_quality_matrix.v1|BLOCKED|actual_path_only||PASS|
|transaction_cost_cash_yield_matrix|transaction_cost_cash_yield_matrix.v1|BLOCKED|actual_path_only||PASS|
|stress_risk_metrics_matrix|stress_risk_metrics_matrix.v1|BLOCKED|actual_path_only||PASS|
|regime_baseline_expansion_matrix|regime_baseline_expansion_matrix.v1|BLOCKED|actual_path_only||PASS|
|no_trade:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|no_trade:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|100_qqq:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|100_qqq:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|qqq_60_sgov_40:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|qqq_60_sgov_40:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|qqq_50_sgov_50:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|qqq_50_sgov_50:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|limited_adjustment:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|limited_adjustment:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|dynamic_v0_5_ai_trend_confirmed_only:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|dynamic_v0_5_ai_trend_confirmed_only:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|limited_adjustment_event_override_v1:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|limited_adjustment_event_override_v1:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1:metrics_actual_path.json|metrics_actual_path.v1|INPUT_ONLY|internal.execution_semantics.actual_path.v1||PASS|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1:metrics_target_path.json|metrics_target_path.v1|BLOCKED|internal.execution_semantics.target_path.v1||PASS|
|legacy_dynamic_result_cannot_unlock_promotion|execution_semantics_promotion_readiness_summary.v1|BLOCKED|actual_path_only||PASS|

## Gate 结论

本报告只用于 research-only owner review；任何 positive row 都不是 paper-shadow、production、broker 或 dynamic promotion approval。
