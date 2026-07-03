# High-Intensity Trigger Selection Criteria

本报告定义 high-intensity risk-cap trigger 的候选标准。这些标准只用于 2335 threshold selection，不是 production policy。

- criteria_id: `high_intensity_risk_cap_forward_observe_v1`
- source_signal_family: `volatility_regime_scope_narrowed_risk_cap`
- selected_usage: `high_intensity_forward_observe`
- low_intensity: `record_only`
- medium_intensity: `record_only`
- high_intensity: `forward_observe_event_candidate`

## Candidate Thresholds

- `P90_RISK_CAP_SCORE`: `INTENSITY_PERCENTILE_THRESHOLD` / `P90`
- `P95_RISK_CAP_SCORE`: `INTENSITY_PERCENTILE_THRESHOLD` / `P95`
- `COMPOSITE_HIGH_INTENSITY_RULE`: `COMPOSITE_HIGH_INTENSITY_RULE` / `risk_cap_triggered AND scope_active AND risk_cap_score >= 1.0 AND signal_direction != none`

禁止用途包括 automatic exposure cap、target weight action、rebalance instruction、paper-shadow、production 和 broker action。
