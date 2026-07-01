# Regime State Machine Design Audit

TRADING-2315 只定义 diagnostic-only regime state machine design，不生成 label series 或交易信号。

- status: `REGIME_STATE_MACHINE_DESIGN_AUDIT_READY_DIAGNOSTIC_ONLY`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `owner_static_design_audit`
- data_quality_status: `NOT_APPLICABLE_STATIC_DESIGN_AUDIT`
- label_count: `9`
- transition_rule_count: `8`
- guardrail_count: `6`
- candidate_segmentation_use_case_count: `4`
- next_task: `TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC`
- candidate_signal_generated: `False`
- regime_label_series_generated: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- dynamic_promotion_status: `BLOCKED`

## Label Taxonomy

|label_id|regime_group|pit_requirement|lookahead_risk|
|---|---|---|---|
|`uptrend`|`constructive`|`ex_ante_trend_features_only`|`low_if_defined_with_lagged_trend_features`|
|`late_uptrend`|`constructive_fragile`|`ex_ante_trend_and_fragility_features_only`|`medium_if_defined_with_future_peak_or_drawdown`|
|`drawdown`|`defensive`|`observed_to_date_drawdown_only`|`high_if_final_trough_or_full_episode_is_used`|
|`panic`|`defensive_extreme`|`observed_to_date_volatility_and_drawdown_only`|`high_if_crash_event_is_labeled_after_the_fact`|
|`rebound`|`recovery`|`lagged_recovery_confirmation_only`|`high_if_future_recovery_path_is_required`|
|`failed_rebound`|`recovery_failure`|`lagged_failure_confirmation_only`|`high_if_failure_is_known_only_after_future_decline`|
|`range_bound`|`neutral_choppy`|`trailing_range_and_trend_features_only`|`medium_if_defined_by_full_future_range`|
|`high_volatility`|`volatility`|`known_realized_or_pit_implied_volatility_only`|`medium_if_future_volatility_window_is_used`|
|`low_volatility`|`volatility`|`known_realized_or_pit_implied_volatility_only`|`medium_if_future_volatility_window_is_used`|

## Transition Design

|transition_id|from_state|to_state|blocked_confirmation|
|---|---|---|---|
|`constructive_to_fragile`|`uptrend`|`late_uptrend`|`future_peak_or_future_drawdown`|
|`fragile_to_drawdown`|`late_uptrend`|`drawdown`|`final_trough_or_future_loss`|
|`drawdown_to_panic`|`drawdown`|`panic`|`post_hoc_crash_event_name`|
|`panic_to_rebound`|`panic`|`rebound`|`future_recovery_path`|
|`rebound_to_failed_rebound`|`rebound`|`failed_rebound`|`future_failed_rebound_outcome`|
|`directional_to_range_bound`|`uptrend_or_drawdown`|`range_bound`|`full_future_sideways_window`|
|`volatility_overlay_high`|`any_state`|`high_volatility`|`future_volatility_window`|
|`volatility_overlay_low`|`any_state`|`low_volatility`|`future_volatility_window`|

## Anti-Lookahead Guardrails

|guardrail_id|blocked_failure_mode|required_control|
|---|---|---|
|`no_future_outcome_labeling`|`future_return_or_future_drawdown_defines_runtime_state`|`labels_must_use_only_features_known_at_decision_time`|
|`no_hindsight_episode_relabeling`|`full_episode_peak_trough_or_recovery_path_rewrites_labels`|`immutable_daily_label_records_with_label_version`|
|`delayed_confirmation_required`|`same_day_state_flip_from_unconfirmed_noise`|`transition_requires_lagged_or_trailing_confirmation`|
|`missing_input_fail_closed`|`missing_trend_or_volatility_input_defaults_to_tradeable_state`|`output_blocked_or_unknown_diagnostic_state`|
|`diagnostic_only_usage`|`regime_state_used_as_buy_sell_or_position_size_signal`|`no_portfolio_weight_no_broker_no_production_effect`|
|`validation_not_promotion`|`positive_regime_segment_result_treated_as_promotion_evidence`|`owner_review_required_before_any_forward_observe_or_promotion_route`|

## Candidate Segmentation Use Cases

|use_case_id|candidate_family|allowed_usage|blocked_usage|
|---|---|---|---|
|`volatility_risk_cap`|`volatility_risk_cap`|`risk_cap_stress_capture_and_false_positive_segmentation`|`replacing_validated_risk_cap_signal`|
|`breadth_proxy`|`breadth_participation_proxy`|`proxy_bias_and_trend_fragility_segmentation`|`promotion_grade_breadth_evidence`|
|`ai_leadership`|`ai_semiconductor_leadership`|`leadership_weakening_and_overweight_risk_context`|`standalone_ai_regime_alpha`|
|`liquidity_rates`|`liquidity_rates_pressure`|`rates_pressure_context_and_exposure_cap_interpretation`|`full_liquidity_credit_pressure_claim`|

## TRADING-2316 Route

- route_status: `READY_FOR_DIAGNOSTIC_POC_DESIGN_ONLY`
- required_before_trading_2316: `define_input_feature_contract,define_known_at_or_lag_policy,run_cached_data_quality_gate_if_cached_data_is_read,write_label_series_schema,write_label_versioning_contract,fail_closed_on_missing_inputs`
- blocked_until_trading_2316: `regime_label_series,candidate_segmentation_metrics,actual_path_validation_by_regime,daily_report_integration,portfolio_or_broker_effect`

## Safety

本报告不读取 cached market / macro data，不生成 regime label series，不运行 backtest / actual-path validation，不写入 daily scoring、portfolio weights、forward observe runtime、production report path 或 broker path。任何后续 TRADING-2316 label generation 必须重新审计 PIT / known-at contract 和 data quality gate。
