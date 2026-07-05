# 动态策略 cost / turnover / cooldown sensitivity

## Executive summary

- status：`DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY`
- source top candidate：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- source decision：`OWNER_REVIEW_REQUIRED`
- top candidate after sensitivity：`dynamic_regime_overlay_v0_4_lower_turnover`
- decision after sensitivity：`OWNER_REVIEW_REQUIRED`
- requested date range：`2022-12-01 to 2026-07-05`
- data quality：`PASS_WITH_WARNINGS`

## Required answers

- 2365 top candidate survives realistic cost：`YES`
- 2365 top candidate survives conservative cost：`YES`
- 2365 top candidate turnover acceptable：`NO`
- cooldown fragility：`NOT_SEVERE`
- ranking changed after sensitivity：`YES`
- upgrade from OWNER_REVIEW_REQUIRED recommended：`NO`

## 安全边界

- 本报告只生成 research evidence，不批准 scheduler、paper-shadow、production 或 broker/order。
- next route：`TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate`