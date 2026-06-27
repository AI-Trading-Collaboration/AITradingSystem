# Hierarchical First-Layer Actual-Path Matrix

- Status: `HIERARCHICAL_FIRST_LAYER_ACTUAL_PATH_READY_PROMOTION_BLOCKED`
- Market regime: `ai_after_chatgpt`
- market_regime: `ai_after_chatgpt`
- default_backtest_start: `2022-12-01`
- probe_count: `4`
- improved_vs_flat_probe_count: `0`
- comparison_scope: `['old_scorecard_first_layer_v1', 'flat_calibrated_first_layer_v1', 'hierarchical_first_layer_v1', 'prior_static_baselines_referenced_from_expanded_universe_artifacts', 'limited_adjustment_referenced_from_expanded_universe_artifacts']`
- flat_probe_metric_source_rows: `8`

Safety boundary:
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Probe Rows

- balanced_dynamic_probe: hierarchical_vs_flat_return_delta=-0.013101, hierarchical_vs_flat_calmar_delta=-0.397017, improved=False
- defensive_overlay_probe: hierarchical_vs_flat_return_delta=-0.020074, hierarchical_vs_flat_calmar_delta=-0.333892, improved=False
- drawdown_control_probe: hierarchical_vs_flat_return_delta=-0.036204, hierarchical_vs_flat_calmar_delta=-0.829747, improved=False
- risk_on_diagnostic_probe: hierarchical_vs_flat_return_delta=-0.004393, hierarchical_vs_flat_calmar_delta=-0.372731, improved=False
