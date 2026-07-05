# 动态策略 research-only shadow observation protocol

## Executive summary

- status：`DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY`
- protocol mode：`RESEARCH_ONLY`
- primary observation candidate：`dynamic_regime_overlay_v0_4_lower_turnover`
- ranking top from 2365：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- robustness top from 2366：`dynamic_regime_overlay_v0_4_lower_turnover`
- gate decision from 2367：`OWNER_REVIEW_REQUIRED`
- research-only shadow observation 和 paper-shadow execution 不同：
  前者只写研究证据和 preview protocol；后者会创建模拟交易状态，本任务禁止。

## 观察内容

- advisory preview、proposed weight delta、valid-until window、risk-cap state、constraint state、no-trade reason。
- expected turnover、cooldown state、cost assumption、static baseline comparison、2365 ranking top comparison。
- owner review flag 和 escalation reason。

## 明确不做

- 不创建 paper trade 或 shadow position。
- 不 append event、不 bind outcome、不 mutate outcome store。
- 不启用 scheduler、不创建 scheduled task、不生成 daily report。
- 不启用 production，不调用 broker，不发送订单。

## Review thresholds

|trigger|condition|action|
|---|---|---|
|`drawdown_trigger`|`candidate_drawdown_materially_worse_than_static_baseline`|`OWNER_REVIEW_REQUIRED`|
|`turnover_trigger`|`expected_turnover_above_owner_accepted_threshold`|`OWNER_REVIEW_REQUIRED`|
|`cost_fragility_trigger`|`edge_disappears_under_realistic_cost_assumptions`|`OWNER_REVIEW_REQUIRED`|
|`divergence_trigger`|`ranking_top_and_robustness_top_disagree_repeatedly`|`OWNER_REVIEW_REQUIRED`|
|`stale_signal_trigger`|`signal_executes_outside_valid_until_window`|`BLOCK_OBSERVATION_AND_REPORT`|
|`guardrail_trigger`|`any_paper_shadow_production_or_broker_flag_true`|`HARD_FAIL`|

## Recommended next route

- next route：`TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_Observation_Dry_Run`