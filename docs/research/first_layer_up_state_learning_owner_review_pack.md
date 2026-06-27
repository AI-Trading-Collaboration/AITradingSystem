# First-Layer Up-State Learning Owner Review Pack

- Status: `FIRST_LAYER_UP_STATE_LEARNING_OWNER_REVIEW_READY_PROMOTION_BLOCKED`
- Market regime: `ai_after_chatgpt`
- market_regime: `ai_after_chatgpt`
- default_backtest_start: `2022-12-01`
- failure_reason: `flat_five_class_model_predicted_zero_constructive_and_risk_on`
- hierarchical_model_implemented: `True`
- risk_off_detector_retained: `True`
- upper_state_detector_predicted_upper_count: `185`
- upper_state_collapse_flag: `True`
- actual_path_status: `UP_STATE_FEATURES_INSUFFICIENT_PROMOTION_BLOCKED`
- promotion_status: `BLOCKED`

Safety boundary:
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Owner Questions

- 当前第一层为什么失败: `flat five-class model predicted zero constructive/risk_on and became over-defensive.`
- 是否已经改成分层模型: `yes, risk-off detector + upper-state detector + severity scaler.`
- risk-off detector 是否保留价值: `yes, it is retained as a separate module.`
- upper-state detector 是否学出上行: `185`
- 接回 actual-path 后是否改善: `0`
- dynamic promotion 为什么仍 blocked: `research-only evidence remains insufficient and owner approval is pending.`
