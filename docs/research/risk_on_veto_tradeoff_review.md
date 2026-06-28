# Risk-On Veto Tradeoff Review

状态：`RISK_ON_VETO_TRADEOFF_READY_OBSERVE_ONLY`

- false_add_risk_cost_reduction: `True`
- avoided_false_add_risk_cost_total: `1.07859`
- captured_upside_lost_total: `3.421701`
- net_veto_benefit_total: `-2.343111`
- risk_on_veto_defensive_regression_reduction: `2`
- return_seeking_diagnostic_over_conservative: `True`

该 tradeoff matrix 只用于 observe-only diagnostic。即使 net benefit 为正，也不能输出 allocation 或进入 promotion。
