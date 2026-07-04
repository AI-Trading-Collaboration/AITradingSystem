# High-Intensity Runtime Event Detection Contract

- selected_rule_id: `COMPOSITE_HIGH_INTENSITY_RULE`
- selected_rule_hash: `c0ade3f13c9d238977b882d062a4d0ba125e05ab1f26ee11dfd9475868dba4ec`
- boolean_expression: `risk_cap_triggered == true AND scope_active == true AND risk_cap_score >= 1.0 AND signal_direction != none`
- known_at_policy: `NEXT_SESSION_DECISION_POLICY`
- strict_pit_ready: `False`

Detection output is observe-only context. Target weight, rebalance, reduce position and broker action outputs are blocked.
