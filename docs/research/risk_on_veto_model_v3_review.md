# Risk-On Veto Model v3 Matrix

状态：`RISK_ON_VETO_MODEL_V3_READY_PROMOTION_BLOCKED`

## 摘要

- `observation_count`: `1343`
- `true_positive`: `226`
- `false_positive`: `305`
- `false_negative`: `342`
- `true_negative`: `470`
- `precision`: `0.425612`
- `recall`: `0.397887`
- `specificity`: `0.606452`
- `positive_rate`: `0.395383`
- `model_type`: `monotonic_scorecard`
- `allowed_families`: `['volatility_compression', 'rates_liquidity']`
- `thresholds`: `{'status': 'pilot_baseline', 'owner': 'research_governance', 'rationale': 'Risk-on veto v3 is a blocker, not a positive risk-on signal. The veto probability threshold requires a modest majority of volatility/rates evidence before growth overlay is blocked, and never authorizes TQQQ.\n', 'review_condition': 'Recalibrate only after observe-only forward diagnostic evidence shows stable false-add-risk reduction without destroying captured upside.\n', 'veto_active_probability_min': 0.55, 'high_confidence_min': 0.6, 'validity_days': 20}`
- `false_add_risk_cost_reduction`: `True`
- `defensive_probe_regression_reduction`: `True`
- `captured_upside_lost`: `-0.002916`
- `tqqq_stress_reduction`: `True`
- `veto_hit_rate`: `0.397887`
- `veto_false_positive_rate`: `0.393548`
- `growth_overlay_enabled`: `False`
- `tqqq_allocation_enabled`: `False`
- `can_emit_weights`: `False`

所有结论均为 research-only diagnostic，不构成 candidate、paper-shadow、production 或 broker action。
