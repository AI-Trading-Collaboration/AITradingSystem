# 动态策略 research-only shadow observation dry-run

## Executive summary

- status：`DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_READY`
- observation mode：`RESEARCH_ONLY_DRY_RUN`
- primary observation candidate：`dynamic_regime_overlay_v0_4_lower_turnover`
- ranking top from 2365：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- robustness top from 2366：`dynamic_regime_overlay_v0_4_lower_turnover`
- observation decision：`OWNER_REVIEW_REQUIRED`
- owner review required：`True`

## Source protocol from TRADING-2368

- protocol loaded：`True`
- field schema loaded：`True`
- review thresholds loaded：`True`

## Observation candidate

- 本次 dry-run 观察 `dynamic_regime_overlay_v0_4_lower_turnover`。
- 观察 robustness top 的原因：TRADING-2367 在收益排名 top 和稳健性 top 分歧后推荐 robustness top 做 research-only 观察。

## Observation dry-run record

- observation id：`TRADING-2369_2026-07-05_dynamic_regime_overlay_v0_4_lower_turnover`
- signal state：`SOURCE_ARTIFACT_PREVIEW_ONLY_NOT_RECOMPUTED`
- no trade reason：`RESEARCH_ONLY_DRY_RUN_NO_EXECUTION`

## Static baseline comparison

- `static_baseline`：decision=`STATIC_BASELINE_REFERENCE`，dynamic_vs_static_gap=`0.0`，max_drawdown=`-0.140068`，turnover=`0.0`。

## Ranking top vs robustness top comparison

- `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`：decision=`OWNER_REVIEW_REQUIRED`，dynamic_vs_static_gap=`0.021302`，max_drawdown=`-0.183642`，turnover=`1.964574`。
- `dynamic_regime_overlay_v0_4_lower_turnover`：decision=`OWNER_REVIEW_REQUIRED`，dynamic_vs_static_gap=`0.002205`，max_drawdown=`-0.122866`，turnover=`2.04`。

## Review flags and thresholds

- review reason：`TRADING-2367 gate decision remains OWNER_REVIEW_REQUIRED; turnover requires owner review after TRADING-2366; ranking top and robustness top diverge`
- escalation flag：`OWNER_REVIEW_REQUIRED`

## No-side-effect evidence

- 是否生成 paper trade：否。
- 是否创建 shadow position：否。
- 是否写 event：否。
- 是否 bind outcome：否。
- 是否生成 daily report：否。
- 是否触发 production / broker：否。

## Explicit non-goals

- 不启用 scheduler，不创建 scheduled task。
- 不 append event，不 bind outcome，不 mutate outcome store。
- 不启用 paper-shadow，不创建 paper trade 或 shadow position。
- 不进入 production，不调用 broker，不发送 order。

## Recommended next route

- next route：`TRADING-2370_Dynamic_Strategy_Research_Only_Shadow_Observation_Replay_No_Side_Effect_Validation`