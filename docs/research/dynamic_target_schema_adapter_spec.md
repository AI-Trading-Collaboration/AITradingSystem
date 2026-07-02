# Dynamic Target Schema Adapter Spec

- adapter_row_count: `116`
- wrapper_generated: `True`

## Mapping Rules

- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `baseline_id` via `derive_wrapper_metadata`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `source_id` via `no fallback allowed`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `source_type` via `no fallback allowed`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `source_path` via `no fallback allowed`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `source_hash` -> `source_hash` via `bind_inventory_source_hash`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `date` -> `date` via `copy_if_non_actionable_research_artifact`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `target_asset` -> `target_asset` via `copy_if_non_actionable_research_artifact`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `target_exposure` -> `target_exposure` via `copy_if_non_actionable_research_artifact`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `target_exposure` -> `risk_asset_exposure` via `copy_if_non_actionable_research_artifact`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `asset_weight` -> `asset_weight` via `copy_if_non_actionable_research_artifact`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `cash_weight` via `leave_null_if_source_does_not_emit_cash_allocation`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `as_of_timestamp` via `derive_date_level_timestamp_with_pit_caveat`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `decision_timestamp` via `derive_date_level_decision_timestamp_with_pit_caveat`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `valid_from` via `derive_from_date_with_validity_caveat`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `valid_until` via `derive_from_date_with_validity_caveat`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `rebalance_flag` via `default_false_no_rebalance_instruction`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `rebalance_timestamp` via `derive_from_decision_timestamp`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `source_hash` -> `source_artifact_hash` via `bind_inventory_source_hash`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `signal_source_id` -> `signal_source_id` via `copy_if_non_actionable_research_artifact`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `advisory_id` via `empty_if_not_advisory_source`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `generated_at` via `no fallback allowed`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `baseline_schema_version` via `derive_wrapper_metadata`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `pit_policy` via `derive_wrapper_metadata`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `replayability_status` via `no fallback allowed`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `` -> `known_at_semantics` via `no fallback allowed`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `promotion_allowed` -> `promotion_allowed` via `force_research_safety_value`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `paper_shadow_allowed` -> `paper_shadow_allowed` via `force_research_safety_value`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `production_allowed` -> `production_allowed` via `force_research_safety_value`
- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `broker_action` -> `broker_action` via `force_research_safety_value`
- `gies_execution_semantics_dynamic_v0_5_ai_trend_confirmed_only_target_vs_actual_position_path_csv`: `` -> `baseline_id` via `derive_wrapper_metadata`
