# 动态策略 ranking vs robustness divergence review

- ranking top：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- robustness top：`dynamic_regime_overlay_v0_4_lower_turnover`
- divergence：`True`

## Explanation

- ranking top：2365 ranking top leads because it has the largest valid-until cost-adjusted return and dynamic-vs-static gap, but it accepts higher drawdown and more frequent rebalances.
- robustness top：2366 robustness top ranks first because it keeps lower drawdown, longer average holding days, fewer false risk-off events and positive stress survival, while giving up upside.

## Summary flags

- ranking_top_can_be_made_robust：`YES`
- robustness_top_can_capture_more_upside：`NO`
- fusion_candidate_outperforms_both：`NO`
- paper_shadow_remains_disabled：`true`