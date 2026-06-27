# Dynamic Defensive Overlay Feasibility Review

元数据：

- review_id：`dynamic_strategy_closeout_2026-06-27`
- source_commit：`28cabc10b042bd9da98780070aea9f85d54c5b5d`
- market_regime：`ai_after_chatgpt`
- requested date range：`2022-12-01`～`2026-06-26`
- metric_namespace：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- promotion_status：`BLOCKED`
- owner_review_status：`OWNER_REVIEW_REQUIRED`

## 结论

Dynamic strategy 不应继续作为 full allocation strategy，但其中部分模块仍有 defensive overlay / advisory diagnostic 价值。允许的方向是 observe-only、risk-reduction-only、manual-review-required；不允许自动 risk-on、自动交易、production weight change 或 broker order。

## Module Assessment

|模块|保留价值|允许角色|限制|
|---|---|---|---|
|event risk score|有条件|advisory only|runtime trace 缺 event type/source taxonomy provenance，不可自动触发交易。|
|risk-off override|有条件|observe-only defensive overlay candidate|risk timing 仍为 `RISK_OFF_TOO_NOISY`，只能提示降风险复核。|
|regime diagnosis|中等|advisory diagnostic|regime expansion 显示 dynamic 复杂度未稳定优于简单 baseline。|
|high-vol stress flag|中等|advisory diagnostic|stress gate 对 dynamic variants 仍 blocked。|
|cash / SGOV fallback|中等|manual review advisory|cash/SGOV yield model 是 research approximation，不是生产结算模型。|
|manual review advisory|较高|owner review prompt / observe-only watch|后续任何 watch 仍需 owner 批准。|

## 必答问题

1. 哪些模块仍有独立风控价值？

`regime_diagnosis`、`high_vol_stress_flag`、`manual_review_advisory` 和 observe-only 的 `risk_off_override` 仍有价值。

2. 哪些模块不应控制仓位，只应提供 advisory？

全部模块在当前 closeout 后都不应直接控制仓位。尤其 event risk score、risk-off override 和 cash/SGOV fallback 不得自动写 production weights。

3. Defensive overlay 是否只允许降低风险？

是。允许行为仅限 risk-reduction advisory、manual review required 和 observe-only forward watch。

4. Risk-on 是否必须继续慢确认？

是。Risk-on 自动恢复仍被禁止，任何 risk-on 只能经过慢确认和 owner review。

5. Overlay 是否可以进入 observe-only forward watch？

可以，但只能在 owner 批准后作为下一阶段 `TRADING-1506～1525` 的 observe-only forward watch，不接入 production、paper-shadow 或 broker。
