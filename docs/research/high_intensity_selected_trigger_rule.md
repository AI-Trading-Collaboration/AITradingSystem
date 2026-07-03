# High-Intensity Selected Trigger Rule

- selected_rule_id: `COMPOSITE_HIGH_INTENSITY_RULE`
- selected_rule_version: `v1`
- usage_mode: `high_intensity_forward_observe`
- boolean_expression: `risk_cap_triggered == true AND scope_active == true AND risk_cap_score >= 1.0 AND signal_direction != none`
- threshold_type: `COMPOSITE_HIGH_INTENSITY_RULE`
- threshold_value: `1.0`
- known_at_policy: `NEXT_SESSION_DECISION_POLICY`
- pit_policy: `PIT_APPROXIMATION_READY`

该规则仅用于 research-only forward observe event logger 和 manual review context，不允许自动 exposure cap、target weight、rebalance、paper-shadow、production 或 broker action。