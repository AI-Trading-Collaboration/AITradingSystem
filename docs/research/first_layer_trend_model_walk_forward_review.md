# First-Layer Trend Model Walk-Forward Review

- Status: `FIRST_LAYER_WALK_FORWARD_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- default_backtest_start: `2022-12-01`
- accuracy: `0.361416`
- balanced_accuracy: `0.151597`
- risk_off_precision: `0.672055`
- risk_off_recall: `0.512324`
- false_risk_off_rate: `0.173382`
- late_risk_off_rate: `0.338217`
- false_risk_on_rate: `0.0`
- late_re_risk_rate: `0.021978`
- consensus_label_margin_capture: `0.879124`
- model_id: `first_layer_ordinal_linear_v1`
- split_count: `13`
- prediction_count: `819`
- train_window_days: `504`
- validation_window_days: `63`
- step_days: `21`
- min_train_samples: `300`
- label_horizon_days: `20`
- research_only: `True`
- actual_path_required: `True`
- target_path_metrics_role: `diagnostic_only`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- production_effect: `none`
- manual_review_required: `True`
- dynamic_promotion_status: `BLOCKED`

Walk-forward metrics use validation windows only and keep promotion blocked.
