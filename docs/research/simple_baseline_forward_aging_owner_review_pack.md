# Simple Baseline Forward Aging Owner Review Pack

- 状态：`OWNER_REVIEW_READY`
- primary_candidate：`equal_risk_qqq_sgov`
- challenger_candidate：`dyn_tqqq_capped_trend`
- production_effect：`none`
- broker_action：`none`
- promotion_allowed：`false`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- manual_review_required：`true`

## Required Answers

- `1_primary_forward_aging_candidate`: `equal_risk_qqq_sgov`
- `2_comparator_baselines`: `['qqq_50_sgov_50', 'qqq_60_sgov_40', '100_qqq']`
- `3_challenger`: `dyn_tqqq_capped_trend`
- `4_strategy_definitions_locked`: `True`
- `5_data_quality_gate_passed`: `False`
- `6_paper_shadow_review_threshold`: `['policy definition locked', 'comparator definition locked', 'PIT boundary pass', 'data quality pass', 'forward aging 120d matured observations >= 20', 'primary candidate not clearly dominated by comparator', 'drawdown reduction stable', 'missed upside acceptable', 'turnover acceptable', 'owner manual review approval']`
- `7_remaining_matured_samples_before_paper_shadow_review`: `20`
- `8_continue_pause_tqqq_heavy`: `True`
- `9_continue_block_leaps_wheel`: `True`
- `10_continue_quarantine_tail_risk_fallback`: `True`
- `11_owner_approved_start_forward_aging_observation`: `True`

本报告只允许用于 research-only forward observation，不生成交易建议、订单、paper-shadow activation 或 production config mutation。
