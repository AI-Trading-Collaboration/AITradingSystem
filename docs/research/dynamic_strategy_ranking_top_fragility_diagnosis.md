# Dynamic strategy ranking top fragility diagnosis

- status：`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY`
- ranking top candidate：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`

- `turnover_risk`：`evaluate=True`；stress monthly turnover caps and single-step deltas
- `drawdown_risk`：`evaluate=True`；compare drawdown against lower-turnover reference
- `cooldown_fragility`：`evaluate=True`；test cooldown and min-holding variants
- `cost_fragility`：`evaluate=True`；run base, realistic, conservative and harsh cost stress
- `stale_signal_risk`：`evaluate=True`；preserve valid-until and test expiry decay
