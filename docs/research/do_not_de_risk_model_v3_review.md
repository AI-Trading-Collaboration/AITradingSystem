# Do-Not-De-Risk Model v3 Matrix

状态：`DO_NOT_DERISK_MODEL_V3_READY_PROMOTION_BLOCKED`

## 摘要

- `observation_count`: `1343`
- `true_positive`: `130`
- `false_positive`: `389`
- `false_negative`: `177`
- `true_negative`: `647`
- `precision`: `0.250482`
- `recall`: `0.423453`
- `specificity`: `0.624517`
- `positive_rate`: `0.386448`
- `model_type`: `monotonic_scorecard`
- `allowed_families`: `['drawdown_recovery']`
- `thresholds`: `{'status': 'pilot_baseline', 'owner': 'research_governance', 'rationale': 'Drawdown-recovery v3 is a defensive neutralization diagnostic. The active probability threshold requires a modest majority signal before overriding defensive posture, while keeping the channel from becoming add-risk.\n', 'review_condition': 'Recalibrate only after observe-only forward diagnostic evidence is available across at least one additional drawdown/recovery episode.\n', 'do_not_de_risk_active_probability_min': 0.55, 'high_confidence_min': 0.6, 'validity_days': 20}`
- `false_risk_off_cost_reduction`: `False`
- `missed_upside_reduction`: `False`
- `defensive_probe_regression_count`: `0`
- `2022_slice_metrics`: `{'observation_count': 251, 'true_positive': 3, 'false_positive': 56, 'false_negative': 23, 'true_negative': 169, 'precision': 0.050847, 'recall': 0.115385, 'specificity': 0.751111, 'positive_rate': 0.23506, 'year': 2022}`
- `can_emit_add_risk`: `False`
- `can_emit_weights`: `False`

所有结论均为 research-only diagnostic，不构成 candidate、paper-shadow、production 或 broker action。
