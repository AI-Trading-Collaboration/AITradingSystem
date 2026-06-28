# Regime slice summary

- status: `FIRST_LAYER_CURRENT_STATE_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- data_quality_status: `PASS_WITH_WARNINGS`

|slice|role|date_range|signal_obs|coverage_status|false_risk_on|false_risk_off|late_risk_off|late_risk_on|
|---|---|---|---:|---|---:|---:|---:|---:|
|`2022_bear_rate_shock`|stress_comparison_not_primary_ai_conclusion|2022-01-03 to 2022-12-30|0|`NO_SIGNAL_COVERAGE`|0|0|0|0|
|`2023_recovery`|ai_after_chatgpt_recovery|2023-01-03 to 2023-12-29|216|`SIGNAL_COVERED`|89|75|44|39|
|`2024_ai_concentration`|ai_after_chatgpt_concentration|2024-01-02 to 2024-12-31|252|`SIGNAL_COVERED`|102|169|38|71|
|`2025_2026_trial_like_window`|ai_after_chatgpt_recent_trial_like|2025-01-02 to 2026-06-26|309|`SIGNAL_COVERED`|7|255|4|196|

2022 slice 是 stress comparison，不是默认 AI-cycle conclusion window。当前 baseline signal 从 2023-02-22 才开始，因此不能宣称已经验证 2022 bear/rate-shock behavior。
